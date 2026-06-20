/*
    srtla - SRT transport proxy with link aggregation
    Copyright (C) 2020-2021 BELABOX project

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

/* ===========================================================================
   srtla_rec.c - SRTLA-Receiver (laeuft auf dem VPS/Relay-Server).

   AUFGABE
   -------
   Der Receiver nimmt auf einem UDP-Port die gebuendelten SRTLA-Pakete von
   einem oder mehreren Sendern entgegen, fuehrt die einzelnen Links wieder
   zu einem einzigen SRT-Strom zusammen und reicht diesen an einen lokalen
   echten SRT-Server (hier: SLS / srt-live-server) weiter.

   DATENMODELL (siehe Strukturen unten)
   ------------------------------------
   - conn_group_t ("Gruppe"): repraesentiert genau EINEN logischen Stream
     eines Senders. Eine Gruppe wird durch eine 256-Byte-ID identifiziert,
     die je zur Haelfte von Sender und Receiver erzeugt wird (REG1/REG2).
     Jede Gruppe haelt EINEN UDP-Socket zum lokalen SRT-Server (srt_sock).
   - conn_t ("Verbindung/Link"): ein einzelner Netz-Link innerhalb einer
     Gruppe (z. B. ein LTE-Modem). Eine Gruppe hat 1..MAX_CONNS_PER_GROUP
     Links. Alle Links einer Gruppe transportieren denselben SRT-Strom.

   ABLAUF EINES PAKETS
   -------------------
   Sender --(SRTLA, UDP srtla_sock)--> [handle_srtla_data]
        REG1/REG2  -> Registrierung von Gruppe bzw. Link
        KEEPALIVE  -> wird zurueckgespiegelt
        SRT-Daten  -> SRTLA-ACK merken + an g->srt_sock weiterleiten
   SRT-Server --(UDP g->srt_sock)--> [handle_srt_data]
        SRT-ACK    -> an ALLE Links der Gruppe (schnelle Zustellung)
        sonstige   -> an den zuletzt aktiven Link (g->last_addr)

   NEBENLAEUFIGKEIT
   ----------------
   Single-threaded, ereignisgesteuert ueber epoll. Es gibt also keine Locks;
   alle globalen Listen werden nur aus der Hauptschleife heraus veraendert.
   =========================================================================== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <endian.h>
#include <netdb.h>
#include <signal.h>          // NEU: fuer geordnetes Herunterfahren (SIGTERM/SIGINT)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <errno.h>

#include "common.h"

/* ---------------------------------------------------------------------------
   Ressourcen-Grenzen (DoS-Schutz). Diese Werte sind bewusst konservativ.
   --------------------------------------------------------------------------- */
#define MAX_CONNS_PER_GROUP 8     // max. Links pro Stream (z. B. 8 Modems)
#define MAX_GROUPS          200   // max. gleichzeitige Streams insgesamt

/* Aufraeum-/Timeout-Intervalle in Sekunden. */
#define CLEANUP_PERIOD 3   // wie oft connection_cleanup() wirklich laeuft
#define GROUP_TIMEOUT  10  // leere Gruppe ohne Links wird nach 10 s verworfen
#define CONN_TIMEOUT   10  // Link ohne Empfang wird nach 10 s verworfen

/* Wie viele empfangene Datenpakete wir sammeln, bevor wir eine gebuendelte
   SRTLA-ACK an den Sender schicken. Die ACKs steuern beim Sender das
   Lastverteilungs-Fenster (siehe srtla_send.c). */
#define RECV_ACK_INT 10

/* NEU: Empfangs-/Sendepuffer fuer den SRTLA-UDP-Socket (8 MB).
   Begruendung: Bei hohen Bitraten und mehreren Links kann der
   Standard-Kernelpuffer zu klein sein und Pakete still verwerfen, was sich
   als Bildaussetzer zeigt. Der Sender setzt bereits 8 MB Sendepuffer - der
   Receiver zieht hier nach.
   HINWEIS: Als unprivilegierter Dienst (User "nobody") wird der Wert vom
   Kernel auf net.core.rmem_max/wmem_max gedeckelt. Wer den vollen Effekt
   will, hebt diese sysctls an (siehe README). */
#define SOCK_BUF_SIZE (8 * 1024 * 1024)

/* Ein einzelner Netz-Link innerhalb einer Gruppe. */
typedef struct srtla_conn {
  struct srtla_conn *next;       // einfach verkettete Liste innerhalb der Gruppe
  struct sockaddr addr;          // Quelladresse des Senders fuer diesen Link
  time_t last_rcvd;              // Zeitpunkt des letzten Empfangs (fuer Timeout)
  int recv_idx;                  // aktueller Schreibindex in recv_log
  uint32_t recv_log[RECV_ACK_INT]; // gepufferte Sequenznummern (big-endian) fuer die SRTLA-ACK
} conn_t;

/* Eine Verbindungsgruppe = ein logischer Stream eines Senders. */
typedef struct srtla_conn_group {
  struct srtla_conn_group *next; // global verkettete Liste aller Gruppen
  conn_t *conns;                 // Liste der Links dieser Gruppe
  time_t created_at;             // Erstellzeit (fuer GROUP_TIMEOUT leerer Gruppen)
  int srt_sock;                  // UDP-Socket zum lokalen SRT-Server (-1 = noch keiner)
  struct sockaddr last_addr;     // zuletzt aktive Sender-Adresse (Rueckkanal-Ziel)
  char id[SRTLA_ID_LEN];         // 256-Byte-Gruppen-ID
} conn_group_t;

/* Format der gebuendelten SRTLA-ACK, die wir an den Sender schicken. */
typedef struct {
  uint32_t type;                 // SRTLA_TYPE_ACK << 16 (big-endian)
  uint32_t acks[RECV_ACK_INT];   // RECV_ACK_INT bestaetigte Sequenznummern
} srtla_ack_pkt;


/* ---------------------------------------------------------------------------
   Globaler Zustand. Da der Dienst single-threaded laeuft, kommen wir ohne
   Synchronisierung aus.
   --------------------------------------------------------------------------- */
int srtla_sock;                              // UDP-Listener fuer SRTLA-Pakete
struct sockaddr srt_addr;                    // Adresse des lokalen SRT-Servers
const socklen_t addr_len = sizeof(struct sockaddr);

conn_group_t *groups = NULL;                 // Kopf der Gruppenliste
int group_count = 0;                         // Anzahl REGISTRIERTER Gruppen (siehe Hinweis bei group_create)

FILE *urandom;                               // Quelle fuer Zufalls-IDs

/* NEU: Flag fuer geordnetes Herunterfahren. volatile sig_atomic_t ist der
   einzige Typ, den man in einem Signal-Handler sicher schreiben darf. */
static volatile sig_atomic_t should_exit = 0;
static void handle_exit_signal(int sig) {
  (void)sig;          // Parameter bewusst ungenutzt
  should_exit = 1;
}

/* ===========================================================================
   Async-I/O via epoll
   epoll meldet uns, auf welchem Socket Daten lesbar sind. Wir haengen an
   jeden ueberwachten Socket einen "userdata"-Zeiger:
     - NULL            -> der globale srtla_sock (Sender-Richtung)
     - conn_group_t*   -> der srt_sock einer bestimmten Gruppe (Server-Richtung)
   Anhand dieses Zeigers entscheidet die Hauptschleife, welcher Handler laeuft.
   =========================================================================== */
int socket_epoll;

int epoll_add(int fd, uint32_t events, void *userdata) {
  struct epoll_event ev={0};
  ev.events = events;
  ev.data.ptr = userdata;
  return epoll_ctl(socket_epoll, EPOLL_CTL_ADD, fd, &ev);
}

int epoll_rem(int fd) {
  struct epoll_event ev; // fuer Linux < 2.6.9 nicht-NULL noetig; auf modernen Kerneln ignoriert
  return epoll_ctl(socket_epoll, EPOLL_CTL_DEL, fd, &ev);
}


/* ===========================================================================
   Kleine Helfer
   =========================================================================== */
void print_help() {
  fprintf(stderr,
          "Syntax: srtla_rec [-v] SRTLA_LISTEN_PORT SRT_HOST SRT_PORT\n\n"
          "-v      Print the version and exit\n");
}

/* Konstant-zeitiger Vergleich von len Bytes.
   "Konstant-zeitig" heisst: die Laufzeit haengt nicht davon ab, AB WELCHEM
   Byte sich a und b unterscheiden - die Schleife laeuft immer voll durch.
   Das verhindert Timing-Seitenkanaele beim Vergleich der geheimen 256-Byte-
   Gruppen-ID (ein Angreifer koennte sonst die ID Byte fuer Byte erraten).
   Rueckgabe: 0 = gleich, -1 = ungleich. */
int const_time_cmp(const void *a, const void *b, int len) {
  unsigned char diff = 0;
  const unsigned char *ca = (const unsigned char *)a;
  const unsigned char *cb = (const unsigned char *)b;
  for (int i = 0; i < len; i++) {
    diff |= (unsigned char)(ca[i] ^ cb[i]);  // XOR: 0 nur bei identischen Bytes
  }

  return diff ? -1 : 0;
}

/* Sichere Zufallsbytes aus /dev/urandom lesen (fuer die Receiver-Haelfte
   der Gruppen-ID). Liest hartnaeckig weiter, bis len Bytes gefuellt sind. */
int get_random(void *dest, size_t len) {
  unsigned char *p = (unsigned char *)dest;
  while (len) {
    int ret = fread(p, 1, len, urandom);
    if (ret <= 0) return -1;
    p   += ret;
    len -= ret;
  }
  return 0;
}


/* ===========================================================================
   Verwaltung von Gruppen und Verbindungen
   =========================================================================== */

/* Gruppe anhand ihrer 256-Byte-ID finden (konstant-zeitiger Vergleich). */
conn_group_t *group_find_by_id(char *id) {
  for (conn_group_t* g = groups; g != NULL; g = g->next) {
    if (const_time_cmp(g->id, id, SRTLA_ID_LEN) == 0) {
      return g;
    }
  }

  return NULL;
}

/* Eine Sender-Adresse einer Gruppe/Verbindung zuordnen.
   Rueckgabe:
      1 - Adresse gehoert zu einem registrierten Link (*rg, *rc gesetzt)
      0 - Adresse ist die "last_addr" einer Gruppe, aber (noch) kein Link
          (*rg gesetzt, *rc = NULL)
     -1 - Adresse voellig unbekannt (*rg, *rc unveraendert)
   Achtung beim Aufrufer: nur bei Rueckgabe 1 ist *rc gueltig. */
int group_find_by_addr(struct sockaddr *addr, conn_group_t **rg, conn_t **rc) {
  for (conn_group_t* g = groups; g != NULL; g = g->next) {
    for (conn_t *c = g->conns; c != NULL; c = c->next) {
      if (const_time_cmp(&(c->addr), addr, addr_len) == 0) {
        *rg = g;
        *rc = c;
        return 1;
      }
    }
    if (const_time_cmp(&g->last_addr, addr, addr_len) == 0) {
      *rg = g;
      *rc = NULL;
      return 0;
    }
  }

  return -1;
}

/* Neue Gruppe anlegen und vorne in die globale Liste einhaengen.
   Die ID besteht aus der Sender-Haelfte (uebernommen) und einer frisch
   erzeugten Receiver-Haelfte; wir wuerfeln neu, falls (extrem unwahrscheinlich)
   eine ID-Kollision auftritt.

   HINWEIS ZUR ZAEHLUNG: group_count wird hier bewusst NICHT erhoeht. Der
   Zaehler zaehlt nur erfolgreich *registrierte* Gruppen und wird erst am
   Ende von group_reg() (nach erfolgreichem REG2-Versand) inkrementiert,
   waehrend group_destroy() ihn dekrementiert. Fehlerpfade in group_reg(),
   die eine eben erst angelegte Gruppe sofort wieder verwerfen, fassen den
   Zaehler daher gar nicht erst an. */
conn_group_t *group_create(char *sender_id, time_t ts) {
  // Sicherstellen, dass die erzeugte ID kein Duplikat ist - sehr unwahrscheinlich
  char id[SRTLA_ID_LEN];
  memcpy(&id, sender_id, SRTLA_ID_LEN/2);
  do {
    int ret = get_random(&id[SRTLA_ID_LEN/2], SRTLA_ID_LEN/2);
    if (ret != 0) return NULL;
  } while(group_find_by_id(id) != NULL);

  // Speicher fuer die neue Gruppe holen
  conn_group_t *g = malloc(sizeof(conn_group_t));
  if (g == NULL) {
    err("malloc() failed\n");
    return NULL;
  }

  // Mit der oben gebildeten ID initialisieren
  memcpy(&g->id, id, SRTLA_ID_LEN);
  g->conns = NULL;
  g->srt_sock = -1;
  g->created_at = ts;
  g->next = groups;
  groups = g;

  return g;
}

/* Gruppe vollstaendig abbauen: alle Links freigeben, SRT-Socket schliessen,
   aus der globalen Liste aushaengen, Speicher freigeben, Zaehler anpassen.
   prev_link: optionaler Zeiger auf das next-Feld des Vorgaengers (spart die
   erneute Suche, wenn der Aufrufer ihn schon hat); sonst NULL. */
int group_destroy(conn_group_t *g, conn_group_t **prev_link) {
  if (g == NULL) return -1;

  for (conn_t *c = g->conns; c != NULL;) {
    conn_t *next = c->next;
    free(c);
    c = next;
  }

  if (g->srt_sock > 0) {
    epoll_rem(g->srt_sock);
    close(g->srt_sock);
  }

  if (prev_link != NULL) {
    // Der Aufrufer kennt den Vorgaenger-Link bereits
    *prev_link = g->next;
  } else {
    // Selbst suchen und aushaengen
    for (conn_group_t **it = &groups; (*it) != NULL; it = &((*it)->next)) {
      if (*it == g) {
        *it = g->next;
        break;
      }
    } // for
  } // prev_link == NULL

  free(g);

  /* Erstell- und Abbau-Pfade muessen group_count konsistent halten, damit
     der Zaehler nicht abdriftet (siehe Hinweis bei group_create). */
  group_count--;

  return 0;
}

/* Anzahl der Links einer Gruppe zaehlen. */
int group_count_conns(conn_group_t *g) {
  int count = 0;
  for (conn_t *c = g->conns; c != NULL; c = c->next) {
    count++;
  }
  return count;
}

/* REG1 verarbeiten: eine neue Gruppe registrieren und mit REG2 antworten. */
int group_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  if (group_count >= MAX_GROUPS) {
    err("%s:%d: group count is %d, rejecting group registration\n",
        print_addr(addr), port_no(addr), group_count);
    goto err;
  }

  // Wenn diese Sender-Adresse bereits registriert ist, abbrechen
  conn_group_t *g = NULL;
  conn_t *c;
  int ret = group_find_by_addr(addr, &g, &c);
  if (ret != -1) goto err;

  // Gruppe anlegen (uebernimmt die erste Haelfte der ID aus in_buf+2)
  char *id = in_buf + 2;
  g = group_create(id, ts);
  if (g == NULL) goto err;

  /* Adresse merken, mit der die Gruppe registriert wurde. Solange diese
     Gruppe aktiv ist, darf dieselbe Adresse keine weitere Gruppe anlegen. */
  g->last_addr = *addr;

  // REG2-Paket bauen (2 Byte Typ + vollstaendige 256-Byte-ID)
  char out_buf[SRTLA_TYPE_REG2_LEN];
  uint16_t header = htobe16(SRTLA_TYPE_REG2);
  memcpy(out_buf, &header, sizeof(header));
  memcpy(out_buf + sizeof(header), g->id, SRTLA_ID_LEN);

  // REG2 senden
  ret = sendto(srtla_sock, &out_buf, sizeof(out_buf), 0, addr, addr_len);
  if (ret != sizeof(out_buf)) goto err_destroy;

  info("%s:%d: group %p registered\n", print_addr(addr), port_no(addr), g);

  // Gruppe erst nach vollstaendigem Erfolg zaehlen
  group_count++;

  return 0;

err_destroy:
  /* group_create() haengt die neue Gruppe immer vorne ein, also ist sie hier
     garantiert der Listenkopf. Der Zaehler wurde noch nicht erhoeht. */
  groups = g->next;
  free(g);

err:
  err("%s:%d: group registration failed\n", print_addr(addr), port_no(addr));
  header = htobe16(SRTLA_TYPE_REG_ERR);
  sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
  return -1;
}

/* REG2 verarbeiten: einen einzelnen Link zu einer bestehenden Gruppe
   hinzufuegen und mit REG3 bestaetigen. */
int conn_reg(struct sockaddr *addr, char *in_buf, time_t ts) {
  conn_group_t *g, *tmp;
  conn_t *c;

  char *id = in_buf + 2;
  g = group_find_by_id(id);
  if (g == NULL) {
    // Unbekannte Gruppen-ID -> Sender soll sich neu via REG1 registrieren
    uint16_t header = htobe16(SRTLA_TYPE_REG_NGP);
    sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
    goto err_early;
  }

  /* Ist die Adresse schon registriert, erlauben wir die erneute
     Registrierung zur SELBEN Gruppe, aber nicht zu einer anderen. */
  int ret = group_find_by_addr(addr, &tmp, &c);
  if (ret != -1 && tmp != g) goto err;

  /* FEHLERBEHEBUNG (gegenueber Upstream):
     Wir merken uns, ob wir den Link in diesem Aufruf NEU angelegt haben.
     Nur dann darf der Fehlerpfad ihn wieder aushaengen/freigeben. Im
     Original wurde im Fehlerfall bedingungslos "g->conns = c->next; free(c)"
     ausgefuehrt - bei einem bereits bestehenden Link (der nicht der Kopf
     der Liste ist) zerstoerte das die Liste und gab einen noch benutzten
     Link frei (Use-after-free). */
  int newly_allocated = 0;

  /* ret == 1 bedeutet: dieser Link existiert bereits -> direkt REG3 schicken.
     Sonst legen wir den Link neu an. */
  if (ret != 1) {
    int conn_count = group_count_conns(g);
    if (conn_count >= MAX_CONNS_PER_GROUP) goto err;

    c = malloc(sizeof(conn_t));
    if (c == NULL) {
      err("malloc() failed\n");
      goto err;
    }
    c->addr = *addr;
    c->recv_idx = 0;
    c->last_rcvd = ts;
    c->next = g->conns;
    g->conns = c;            // neuer Link wird Kopf der Liste
    newly_allocated = 1;
  }

  uint16_t header = htobe16(SRTLA_TYPE_REG3);
  ret = sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);
  if (ret != sizeof(header)) {
    /* Nur einen gerade eben neu angelegten Link wieder entfernen; einen
       bereits bestehenden Link lassen wir unangetastet. */
    if (newly_allocated) {
      g->conns = c->next;
      free(c);
    }
    goto err;
  }

  info("%s:%d (group %p): connection registration\n", print_addr(addr), port_no(addr), g);

  // Bei Erfolg: diesen Peer als zuletzt aktiv markieren
  g->last_addr = *addr;

  return 0;

err:
  header = htobe16(SRTLA_TYPE_REG_ERR);
  sendto(srtla_sock, &header, sizeof(header), 0, addr, addr_len);

err_early:
  err("%s:%d: connection registration for group %p failed\n",
      print_addr(addr), port_no(addr), g);
  return -1;
}

/* ===========================================================================
   Die zentralen Netzwerk-Ereignis-Handler

   Ressourcen-Grenzen:
     * Links pro Gruppe   MAX_CONNS_PER_GROUP
     * Gruppen insgesamt  MAX_GROUPS
   =========================================================================== */

/* Daten vom lokalen SRT-Server -> zurueck an den Sender.
   ACKs werden ueber ALLE Links verteilt (damit sie moeglichst schnell
   ankommen), alle anderen Pakete nur ueber den zuletzt aktiven Link. */
void handle_srt_data(conn_group_t *g) {
  char buf[MTU];

  if (g == NULL) return;

  int n = recv(g->srt_sock, &buf, MTU, 0);
  if (n < SRT_MIN_LEN) {
    err("Group %p: failed to read the SRT sock, terminating the group\n", g);
    group_destroy(g, NULL);
    return;
  }

  // ACK
  if (is_srt_ack(buf, n)) {
    // SRT-ACKs ueber alle Links broadcasten, damit sie zuverlaessig ankommen
    for (conn_t *c = g->conns; c != NULL; c = c->next) {
      int ret = sendto(srtla_sock, &buf, n, 0, &c->addr, addr_len);
      if (ret != n) {
        err("%s:%d (group %p): failed to send the SRT ack\n",
            print_addr(&c->addr), port_no(&c->addr), g);
      }
    }
  } else {
    // Alle anderen Pakete ueber den zuletzt benutzten SRTLA-Link senden
    int ret = sendto(srtla_sock, &buf, n, 0, &g->last_addr, addr_len);
    if (ret != n) {
      err("%s:%d (group %p): failed to send the SRT packet\n",
          print_addr(&g->last_addr), port_no(&g->last_addr), g);
    }
  }
}

/* Eine empfangene Sequenznummer fuer die SRTLA-ACK des jeweiligen Links
   puffern. Sind RECV_ACK_INT Nummern gesammelt, schicken wir ein gebuendeltes
   SRTLA-ACK an genau diesen Link zurueck. */
void register_packet(conn_group_t *g, conn_t *c, int32_t sn) {
  // Sequenznummern big-endian speichern, da sie so uebertragen werden
  c->recv_log[c->recv_idx++] = htobe32(sn);

  if (c->recv_idx == RECV_ACK_INT) {
    srtla_ack_pkt ack;
    ack.type = htobe32(SRTLA_TYPE_ACK << 16);
    memcpy(&ack.acks, &c->recv_log, sizeof(c->recv_log));

    int ret = sendto(srtla_sock, &ack, sizeof(ack), 0, &c->addr, addr_len);
    if (ret != sizeof(ack)) {
      err("%s:%d (group %p): failed to send the srtla ack\n",
          print_addr(&c->addr), port_no(&c->addr), g);
    }

    c->recv_idx = 0;
  }
}

/* Daten vom Sender (auf srtla_sock). Behandelt Registrierung, Keepalives und
   eigentliche SRT-Nutzdaten, die an den lokalen SRT-Server weitergereicht
   werden. */
void handle_srtla_data(time_t ts) {
  char buf[MTU];
  int ret;

  // Paket samt Absenderadresse holen
  struct sockaddr srtla_addr;
  socklen_t len = addr_len;
  int n = recvfrom(srtla_sock, &buf, MTU, 0, &srtla_addr, &len);
  if (n < 0) {
    err("Failed to read a srtla packet\n");
    return;
  }

  // SRTLA-Registrierungspakete behandeln
  if (is_srtla_reg1(buf, n)) {
    group_reg(&srtla_addr, buf, ts);
    return;
  }

  if (is_srtla_reg2(buf, n)) {
    conn_reg(&srtla_addr, buf, ts);
    return;
  }

  // Nur Pakete von Adressen akzeptieren, die zu einem Link gehoeren
  conn_t *c;
  conn_group_t *g;
  ret = group_find_by_addr(&srtla_addr, &g, &c);
  if (ret != 1) return;

  // Empfangszeitpunkt des Links aktualisieren (gegen Timeout)
  c->last_rcvd = ts;

  // SRTLA-Keepalives einfach zuruecksenden
  if (is_srtla_keepalive(buf, n)) {
    int ret = sendto(srtla_sock, &buf, n, 0, &srtla_addr, addr_len);
    if (ret != n) {
      err("%s:%d (group %p): failed to send the srtla keepalive\n",
          print_addr(&srtla_addr), port_no(&srtla_addr), g);
    }
    return;
  }

  // Zu kurze Pakete (keine gueltigen SRT-Pakete) verwerfen
  if (n < SRT_MIN_LEN) return;

  // Diesen Peer als zuletzt aktiv merken (Ziel fuer den Rueckkanal)
  g->last_addr = srtla_addr;

  // Empfangene Datenpakete fuer die SRTLA-ACK protokollieren
  int32_t sn = get_srt_sn(buf, n);
  if (sn >= 0) {
    register_packet(g, c, sn);
  }

  // Bei Bedarf den UDP-Socket zum SRT-Server fuer diese Gruppe oeffnen
  if (g->srt_sock < 0) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
      err("Group %p: failed to create an SRT socket\n", g);
      group_destroy(g, NULL);
      return;
    }
    g->srt_sock = sock;

    int ret = connect(sock, &srt_addr, addr_len);
    if (ret != 0) {
      err("Group %p: failed to connect() the SRT socket\n", g);
      group_destroy(g, NULL);
      return;
    }

    ret = epoll_add(sock, EPOLLIN, g);
    if (ret != 0) {
      err("Group %p: failed to add the SRT socket to the epoll\n", g);
      group_destroy(g, NULL);
      return;
    }
  }

  // Nutzdaten an den lokalen SRT-Server weiterreichen
  ret = send(g->srt_sock, &buf, n, 0);
  if (ret != n) {
    err("Group %p: failed to forward the srtla packet, terminating the group\n", g);
    group_destroy(g, NULL);
  }
}

/* ===========================================================================
   Aufraeumen abgelaufener Ressourcen

   Gruppen:
     * neue, noch leere Gruppe: created_at < (ts - GROUP_TIMEOUT)
     * sonstige Gruppen: wenn alle Links abgelaufen sind
   Links:
     * GC, sobald last_rcvd < (ts - CONN_TIMEOUT)
   =========================================================================== */
void connection_cleanup(time_t ts) {
  // Nur alle CLEANUP_PERIOD Sekunden tatsaechlich aufraeumen
  static time_t last_ran = 0;
  if ((last_ran + CLEANUP_PERIOD) > ts) return;
  last_ran = ts;

  if (groups == NULL) return;

  int total_groups = 0, total_conns = 0, removed_groups = 0, removed_conns = 0;

  debug("Started a cleanup run\n");

  /* Wir laufen mit "Zeiger auf den Vorgaenger-Link" durch beide Listen, damit
     wir Elemente sicher aushaengen koennen, ohne den Listenkopf zu verlieren. */
  conn_group_t *next_g = NULL;
  conn_group_t **prev_g = &groups;
  for (conn_group_t *g = groups; g != NULL; g = next_g) {
    total_groups++;
    next_g = g->next;

    conn_t *next_c = NULL;
    conn_t **prev_c = &g->conns;
    for (conn_t *c = g->conns; c != NULL; c = next_c) {
      total_conns++;
      next_c = c->next;
      if ((c->last_rcvd + CONN_TIMEOUT) < ts) {
        removed_conns++;
        info("%s:%d (group %p): connection removed (timed out)\n",
             print_addr(&c->addr), port_no(&c->addr), g);
        *prev_c = next_c;
        free(c);
        continue;
      }
      prev_c = &c->next;
    }

    // Leere Gruppe, die lange genug leer ist, entfernen
    if (g->conns == NULL && (g->created_at + GROUP_TIMEOUT) < ts) {
      removed_groups++;
      info("Group %p: removed (no connections)\n", g);
      group_destroy(g, prev_g);
      continue;
    }
    prev_g = &g->next;
  }

  debug("Clean up run ended. Counted %d groups and %d connections. "
        "Removed %d groups and %d connections\n",
        total_groups, total_conns, removed_groups, removed_conns);
}

/* ===========================================================================
   SRT ist verbindungsorientiert und antwortet auf unsere Pakete erst, wenn
   wir einen Handshake beginnen. Beim Start probieren wir das fuer jede
   aufgeloeste Adresse durch, um zu pruefen, ob ueberhaupt ein SRT-Server
   erreichbar ist.

   Rueckgabe: -1 = Fehler
               0 = Adresse aufgeloest, SRT scheint nicht erreichbar
               1 = Adresse aufgeloest, SRT antwortet
   =========================================================================== */
int resolve_srt_addr(char *host, char *port) {
  // SRT-Handshake-Induction-Paket vorbereiten
  srt_handshake_t hs_packet = {0};
  hs_packet.header.type = htobe16(SRT_TYPE_HANDSHAKE);
  hs_packet.version = htobe32(4);
  hs_packet.ext_field = htobe16(2);
  hs_packet.handshake_type = htobe32(1);

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  struct addrinfo *srt_addrs;
  int ret = getaddrinfo(host, port, &hints, &srt_addrs);
  if (ret != 0) {
    fprintf(stderr, "Failed to resolve the address %s:%s\n", host, port);
    return -1;
  }

  int tmp_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (tmp_sock < 0) {
    perror("failed to create a UDP socket");
    freeaddrinfo(srt_addrs);          // NEU: kein Leak im Fehlerfall
    return -1;
  }

  // 1 Sekunde auf eine Antwort warten, sonst naechste Adresse probieren
  struct timeval to = { .tv_sec = 1, .tv_usec = 0};
  ret = setsockopt(tmp_sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
  if (ret != 0) {
    perror("failed to set a socket timeout");
    close(tmp_sock);                  // NEU: Socket schliessen
    freeaddrinfo(srt_addrs);          // NEU: kein Leak im Fehlerfall
    return -1;
  }

  int found = -1;
  for (struct addrinfo *addr = srt_addrs; addr != NULL && found == -1; addr = addr->ai_next) {
    info("Trying to connect to SRT at %s:%s... ", print_addr(addr->ai_addr), port);
    /* Wird nicht auf jedem Log-Level ausgegeben; ein flush schadet aber
       nicht, falls nichts ausgegeben wurde. */
    fflush(stderr);

    ret = connect(tmp_sock, addr->ai_addr, addr->ai_addrlen);
    if (ret == 0) {
      ret = send(tmp_sock, &hs_packet, sizeof(hs_packet), 0);
      if (ret == sizeof(hs_packet)) {
        char buf[MTU];
        ret = recv(tmp_sock, &buf, MTU, 0);
        if (ret == sizeof(hs_packet)) {
          info("success\n");
          srt_addr = *addr->ai_addr;
          found = 1;
        }
      } // ret == sizeof(buf)
    } // ret == 0

    if (found == -1) {
      info("error\n");
    }
  }
  close(tmp_sock);

  if (found == -1) {
    // Keine Antwort - trotzdem mit der ersten Adresse weitermachen (Warnung)
    srt_addr = *srt_addrs->ai_addr;
    fprintf(stderr, "WARNING: Failed to confirm that a SRT server is reachable at any address\n"
                    "Proceeding with the first address %s\n", print_addr(&srt_addr));
    found = 0;
  }

  freeaddrinfo(srt_addrs);

  return found;
}

/* NEU: Empfangs-/Sende-Puffer eines Sockets best-effort vergroessern.
   Schlaegt es fehl (z. B. wegen net.core.rmem_max), ist das nicht fatal -
   wir geben nur eine Warnung aus und laufen weiter. */
static void tune_socket_buffers(int fd) {
  int sz = SOCK_BUF_SIZE;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz)) != 0) {
    err("warning: could not set SO_RCVBUF to %d bytes\n", sz);
  }
  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz)) != 0) {
    err("warning: could not set SO_SNDBUF to %d bytes\n", sz);
  }
}

#define ARG_LISTEN_PORT (argv[1])
#define ARG_SRT_HOST    (argv[2])
#define ARG_SRT_PORT    (argv[3])
int main(int argc, char **argv) {
  // Kommandozeile auswerten
  if (argc == 2 && strcmp(argv[1], "-v") == 0) {
    printf(VERSION "\n");
    exit(0);
  }
  if (argc != 4) exit_help();

  struct sockaddr_in listen_addr;

  int srtla_port = parse_port(ARG_LISTEN_PORT);
  if (srtla_port < 0) exit_help();

  // Pruefen, ob der SRT-Server erreichbar ist
  int ret = resolve_srt_addr(ARG_SRT_HOST, ARG_SRT_PORT);
  if (ret < 0) {
    exit(EXIT_FAILURE);
  }

  // urandom liefert die Zufallsbytes fuer die Receiver-Haelfte der Gruppen-IDs
  urandom = fopen("/dev/urandom", "rb");
  if (urandom == NULL) {
    perror("failed to open urandom\n");
    exit(EXIT_FAILURE);
  }

  /* NEU: Geordnetes Herunterfahren. systemd schickt beim Stoppen SIGTERM;
     so beenden wir die Schleife sauber, statt einfach getoetet zu werden. */
  signal(SIGTERM, handle_exit_signal);
  signal(SIGINT,  handle_exit_signal);
  signal(SIGPIPE, SIG_IGN);   // ein geschlossener Socket darf uns nicht killen

  // epoll fuer ereignisgesteuerte Netzwerk-I/O
  socket_epoll = epoll_create(1000); // Zahl seit Linux 2.6.8 ohne Bedeutung
  if (socket_epoll < 0) {
    perror("epoll creation failed\n");
    exit(EXIT_FAILURE);
  }

  // Listener-Socket fuer eingehende SRTLA-Pakete einrichten
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(srtla_port);
  srtla_sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (srtla_sock < 0) {
    perror("socket creation failed");
    exit(EXIT_FAILURE);
  }

  /* NEU: SO_REUSEADDR erlaubt sofortiges Neu-Binden nach einem Neustart,
     ohne auf die Freigabe des Ports durch den Kernel warten zu muessen. */
  int one = 1;
  if (setsockopt(srtla_sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
    err("warning: could not set SO_REUSEADDR\n");
  }

  // NEU: groessere Socket-Puffer gegen Paketverluste bei hoher Bitrate
  tune_socket_buffers(srtla_sock);

  ret = bind(srtla_sock, (const struct sockaddr *)&listen_addr, addr_len);
  if (ret < 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }

  ret = epoll_add(srtla_sock, EPOLLIN, NULL);
  if (ret != 0) {
    perror("failed to add the srtla sock to the epoll\n");
    exit(EXIT_FAILURE);
  }

  info("srtla_rec is now running\n");

  // ----- Hauptschleife: auf Netzwerk-Ereignisse warten und verteilen -----
  while(!should_exit) {
    #define MAX_EPOLL_EVENTS 10
    struct epoll_event events[MAX_EPOLL_EVENTS];
    // bis zu 1000 ms blockieren; danach laeuft trotzdem das Cleanup
    int eventcnt = epoll_wait(socket_epoll, events, MAX_EPOLL_EVENTS, 1000);

    // EINTR (durch ein Signal unterbrochen) ist kein Fehler -> erneut pruefen
    if (eventcnt < 0) {
      if (errno == EINTR) continue;
      err("epoll_wait failed: %s\n", strerror(errno));
      continue;
    }

    time_t ts = 0;
    int ret = get_seconds(&ts);
    if (ret != 0) {
      err("Failed to get the timestamp\n");
    }

    int group_cnt;
    for (int i = 0; i < eventcnt; i++) {
      group_cnt = group_count;
      if (events[i].data.ptr == NULL) {
        // userdata == NULL -> Ereignis auf dem globalen SRTLA-Listener
        handle_srtla_data(ts);
      } else {
        // userdata == Gruppe -> Ereignis auf dem SRT-Socket dieser Gruppe
        handle_srt_data((conn_group_t*)events[i].data.ptr);
      }

      /* Falls wir eine Gruppe wegen eines Socket-Fehlers entfernt haben,
         koennten in events[] noch Ereignisse stehen, die auf jetzt
         freigegebenen Speicher zeigen. In dem Fall die Schleife abbrechen
         und eine frische Liste von epoll_wait() holen. */
      if (group_count < group_cnt) break;
    } // for

    connection_cleanup(ts);
  } // while

  /* NEU: geordnetes Herunterfahren - alle Gruppen abbauen und Ressourcen
     freigeben. Nicht zwingend noetig (der Prozess endet ohnehin), aber sauber
     fuer die Logs und fuer Werkzeuge wie valgrind. */
  info("srtla_rec is shutting down\n");
  while (groups != NULL) {
    group_destroy(groups, NULL);
  }
  close(srtla_sock);
  if (urandom) fclose(urandom);
  close(socket_epoll);

  return 0;
}
