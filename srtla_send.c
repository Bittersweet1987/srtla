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
   srtla_send.c - SRTLA-Sender (laeuft auf der Streaming-Seite, z. B. Belabox).

   AUFGABE
   -------
   Eine lokale SRT-Anwendung (OBS, srt-live-transmit, ...) streamt im
   Caller-Modus an SRT_LISTEN_PORT auf diesem Rechner. Der Sender verteilt
   die einzelnen UDP-Pakete dieses Stroms auf mehrere Netzwerk-Links
   (mehrere Modems, jeweils per Quell-IP gebunden) und schickt sie an den
   SRTLA-Receiver. Antworten des Receivers/SRT-Servers werden zurueck an die
   lokale SRT-Anwendung gereicht.

   LASTVERTEILUNG (Kernidee)
   -------------------------
   Pro Link wird ein dynamisches "Fenster" (window) gefuehrt, das grob die
   Kapazitaet des Links abbildet - aehnlich wie bei der TCP-Staukontrolle.
   Zusammen mit der Anzahl "in-flight" (gesendet, aber noch nicht bestaetigt)
   Pakete ergibt sich ein Score; das naechste Paket geht an den Link mit dem
   besten Score (siehe select_conn()). NAKs (Verluste) verkleinern das
   Fenster, ACKs vergroessern es langsam wieder.

   Hinweis: Dieser Sender ist fuer dein Setup zweitrangig (auf der Belabox
   laeuft i. d. R. die dort mitgelieferte Version). Er wird hier vollstaendig
   mitgepflegt und kommentiert, damit das Gesamtbild stimmt und der Code als
   Referenz dient.
   =========================================================================== */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "common.h"

#define PKT_LOG_SZ 256      // Ringpuffergroesse pro Link fuer gesendete Seq.-Nummern
#define CONN_TIMEOUT 4      // Link gilt nach 4 s ohne Empfang als ausgefallen
#define REG2_TIMEOUT 4      // Wartezeit auf REG2 nach gesendetem REG1
#define REG3_TIMEOUT 4      // Wartezeit auf REG3 nach gesendetem REG2
#define GLOBAL_TIMEOUT 10   // alle Links aus -> nach 10 s naechste Receiver-Adresse/Exit
#define IDLE_TIME 1         // nach 1 s Sendepause ein Keepalive schicken

#define SEND_BUF_SIZE (8 * 1024 * 1024)   // 8 MB Sendepuffer je Link-Socket

#define min(a, b) ((a < b) ? a : b)
#define max(a, b) ((a > b) ? a : b)
#define min_max(a, l, h) (max(min((a), (h)), (l)))

/* Fenster-Parameter der Lastverteilung. Das Fenster wird intern mit dem
   Faktor WINDOW_MULT skaliert gefuehrt, um ohne Gleitkomma feiner regeln zu
   koennen. */
#define WINDOW_MIN 1
#define WINDOW_DEF 20
#define WINDOW_MAX 60
#define WINDOW_MULT 1000
#define WINDOW_DECR 100    // Fensterverkleinerung bei einem NAK (Verlust)
#define WINDOW_INCR 30     // Fenstervergroesserung bei einer SRTLA-ACK

#define LOG_PKT_INT 20     // wie oft (in Schleifendurchlaeufen) Debug-Status geloggt wird

/* Ein einzelner ausgehender Link. */
typedef struct conn {
  struct conn *next;
  int fd;                  // UDP-Socket dieses Links (-1 = geschlossen)
  time_t last_rcvd;        // letzter Empfang vom Receiver (Timeout/Reconnect)
  time_t last_sent;        // letzter eigener Versand (fuer Keepalive-Takt)
  struct sockaddr src;     // Quell-IP, an die dieser Link gebunden wird
  int removed;             // Markierung beim Neu-Einlesen der IP-Liste (SIGHUP)
  int in_flight_pkts;      // gesendet, aber noch nicht bestaetigt
  int window;              // aktuelles Kapazitaetsfenster (skaliert mit WINDOW_MULT)
  int pkt_idx;             // Schreibindex in pkt_log
  int pkt_log[PKT_LOG_SZ]; // Ringpuffer gesendeter Sequenznummern (-1 = leer/bestaetigt)
} conn_t;

char *source_ip_file = NULL;

int do_update_conns = 0;   // wird vom SIGHUP-Handler gesetzt (IP-Liste neu lesen)

struct addrinfo *addrs;    // aufgeloeste Adressen des Receivers (mehrere moeglich)

struct sockaddr srtla_addr, srt_addr;
const socklen_t addr_len = sizeof(srtla_addr);
conn_t *conns = NULL;
int listenfd;              // lokaler Socket, auf dem die SRT-Anwendung sendet
int active_connections = 0;
int has_connected = 0;     // ob ueberhaupt jemals eine Verbindung stand

conn_t *pending_reg2_conn = NULL;  // Link, fuer den wir gerade auf REG2 warten
time_t pending_reg_timeout = 0;

char srtla_id[SRTLA_ID_LEN];       // unsere Gruppen-ID (Senderhaelfte zufaellig)


/* ===========================================================================
   Async-I/O via select()
   Der Sender nutzt select() (statt epoll), weil die Zahl der Sockets klein
   und fix ist. active_fds enthaelt alle zu ueberwachenden Deskriptoren.
   =========================================================================== */
fd_set active_fds;
int max_act_fd = -1;

int add_active_fd(int fd) {
  if (fd < 0) return -1;

  if (fd > max_act_fd) max_act_fd = fd;
  FD_SET(fd, &active_fds);

  return 0;
}

int remove_active_fd(int fd) {
  if (fd < 0) return -1;

  FD_CLR(fd, &active_fds);

  return 0;
}


/* ===========================================================================
   Kleine Helfer
   =========================================================================== */
void print_help() {
  fprintf(stderr,
          "Syntax: srtla_send SRT_LISTEN_PORT SRTLA_HOST SRTLA_PORT BIND_IPS_FILE\n\n"
          "-v      Print the version and exit\n");
}


/* ===========================================================================
   SRTLA-Registrierungs-Helfer (REG1/REG2 senden)
   =========================================================================== */
int send_reg1(conn_t *c) {
  if (c->fd < 0) return -1;

  char buf[MTU];
  uint16_t packet_type = htobe16(SRTLA_TYPE_REG1);
  memcpy(buf, &packet_type, sizeof(packet_type));
  memcpy(buf + sizeof(packet_type), srtla_id, SRTLA_ID_LEN);

  int ret = sendto(c->fd, buf, SRTLA_TYPE_REG1_LEN, 0, &srtla_addr, addr_len);
  if (ret != SRTLA_TYPE_REG1_LEN) return -1;

  return 0;
}

int send_reg2(conn_t *c) {
  if (c->fd < 0) return -1;

  char buf[SRTLA_TYPE_REG2_LEN];
  uint16_t packet_type = htobe16(SRTLA_TYPE_REG2);
  memcpy(buf, &packet_type, sizeof(packet_type));
  memcpy(buf + sizeof(packet_type), srtla_id, SRTLA_ID_LEN);

  int ret = sendto(c->fd, buf, SRTLA_TYPE_REG2_LEN, 0, &srtla_addr, addr_len);
  return (ret == SRTLA_TYPE_REG2_LEN) ? 0 : -1;
}


/* ===========================================================================
   Pakete von der lokalen SRT-Anwendung (Caller) verarbeiten
   =========================================================================== */

/* Eine gesendete Sequenznummer im Ringpuffer des Links vermerken und
   in_flight hochzaehlen. */
void reg_pkt(conn_t *c, int32_t packet) {
  debug("%s (%p): register packet %d at idx %d\n",
        print_addr(&c->src), c, packet, c->pkt_idx);
  c->pkt_log[c->pkt_idx] = packet;
  c->pkt_idx++;
  c->pkt_idx %= PKT_LOG_SZ;

  c->in_flight_pkts++;
}

int conn_timed_out(conn_t *c, time_t ts) {
  return (c->last_rcvd + CONN_TIMEOUT) < ts;
}

/* Den besten Link fuer das naechste Paket waehlen.
   Score = window / (in_flight + 1): viel freie Kapazitaet und wenige
   unbestaetigte Pakete -> hoher Score. Abgelaufene Links werden uebersprungen. */
conn_t *select_conn() {
  conn_t *min_c = NULL;
  int max_score = -1;
  int max_window = 0;

  for (conn_t *c = conns; c != NULL; c = c->next) {
    if (c->window > max_window) {
      max_window = c->window;
    }
  }

  time_t t;
  assert(get_seconds(&t) == 0);

  for (conn_t *c = conns; c != NULL; c = c->next) {
    /* Sehr langsame Links koennten wir ganz ignorieren. Dann muesste man sie
       aber periodisch wieder antesten, sonst bleibt ein wegen einer kurzen
       Stoerung deaktivierter Link evtl. fuer immer aus. Daher hier
       auskommentiert belassen. */
    /*if (c->window < max_window / 5) {
      c->window++;
      continue;
    }*/

    if (conn_timed_out(c, t)) {
      debug("%s (%p): is timed out, ignoring it\n", print_addr(&c->src), c);
      continue;
    }

    int score = c->window / (c->in_flight_pkts + 1);
    if (score > max_score) {
      min_c = c;
      max_score = score;
    }
  }

  if (min_c) {
    min_c->last_sent = t;
  }

  return min_c;
}

/* Daten von der lokalen SRT-Anwendung -> ueber den gewaehlten Link an den
   Receiver schicken und die Sequenznummer vermerken. */
void handle_srt_data(int fd) {
  char buf[MTU];
  socklen_t len = sizeof(srt_addr);
  int n = recvfrom(fd, &buf, MTU, 0, &srt_addr, &len);

  conn_t *c = select_conn();
  if (c) {
    int32_t sn = get_srt_sn(buf, n);
    int ret = sendto(c->fd, &buf, n, 0, &srtla_addr, addr_len);
    if (ret == n) {
      if (sn >= 0) {
        reg_pkt(c, sn);
      }
    } else {
      /* Schlaegt das Senden fehl, deaktivieren wir den Link, bis ein
         Reconnect bestaetigt ist. last_rcvd=1 (statt 0), damit
         connection_housekeeping() seine Meldung ausgibt. */
      c->last_rcvd = 1;
      err("%s (%p): sendto() failed, disabling the connection\n",
          print_addr(&c->src), c);
    }
  }
}


/* ===========================================================================
   Pakete vom Receiver verarbeiten
   =========================================================================== */

/* Index im Ringpuffer um "increment" verschieben (mit Wrap-around). */
int get_pkt_idx(int idx, int increment) {
  idx = idx + increment;
  if (idx < 0) idx += PKT_LOG_SZ;
  idx %= PKT_LOG_SZ;
  assert(idx >= 0 && idx < PKT_LOG_SZ);
  return idx;
}

/* Ein per SRT-NAK gemeldetes verlorenes Paket suchen. Auf dem Link, der es
   gesendet hat, das Fenster verkleinern (Verlust = Hinweis auf Ueberlast). */
void register_nak(int32_t packet) {
  for (conn_t *c = conns; c != NULL; c = c->next) {
    int idx = get_pkt_idx(c->pkt_idx, -1);
    for (int i = idx; i != c->pkt_idx; i = get_pkt_idx(i, -1)) {
      if (c->pkt_log[i] == packet) {
        c->pkt_log[i] = -1;
        // Alternativ exponentieller Abfall: c->window = c->window * 998 / 1000;
        c->window -= WINDOW_DECR;
        c->window = max(c->window, WINDOW_MIN*WINDOW_MULT);
        debug("%s (%p): found NAKed packet %d in the log\n",
              print_addr(&c->src), c, packet);
        return;
      }
    }
  }

  debug("Didn't find NAKed packet %d in our logs\n", packet);
}

/* Eine SRTLA-ACK des Receivers verarbeiten: bestaetigtes Paket finden,
   in_flight reduzieren und das Fenster langsam vergroessern. */
void register_srtla_ack(int32_t ack) {
  int found = 0;

  for (conn_t *c = conns; c != NULL; c = c->next) {
    int idx = get_pkt_idx(c->pkt_idx, -1);
    for (int i = idx; i != c->pkt_idx && !found; i = get_pkt_idx(i, -1)) {
      if (c->pkt_log[i] == ack) {
        found = 1;
        if (c->in_flight_pkts > 0) {
          c->in_flight_pkts--;
        }
        c->pkt_log[i] = -1;

        if (c->in_flight_pkts*WINDOW_MULT > c->window) {
          c->window += WINDOW_INCR - 1;
        }

        break;
      }
    }

    /* Jeder aktive Link bekommt pro ACK eine kleine Grundvergroesserung,
       gedeckelt auf WINDOW_MAX. */
    if (c->last_rcvd != 0) {
      c->window += 1;
      c->window = min(c->window, WINDOW_MAX*WINDOW_MULT);
    }
  }
}

/*
  TODO (aus dem Original): Nach einem Ueberlauf der Sequenznummer sollten wir
  ggf. auch hohe Sequenznummern als empfangen markieren. Normalerweise kein
  Problem, da SRTLA-ACKs jedes Paket einzeln bestaetigen und alte Eintraege
  im Ringpuffer ohnehin bald ueberschrieben werden.
*/
void conn_register_srt_ack(conn_t *c, int32_t ack) {
  int count = 0;
  int idx = get_pkt_idx(c->pkt_idx, -1);
  for (int i = idx; i != c->pkt_idx; i = get_pkt_idx(i, -1)) {
    if (c->pkt_log[i] < ack) {
      c->pkt_log[i] = -1;
    } else {
      count++;
    }
  }
  c->in_flight_pkts = count;
}

void register_srt_ack(int32_t ack) {
  for (conn_t *c = conns; c != NULL; c = c->next) {
    conn_register_srt_ack(c, ack);
  }
}

/* Zentrale Verarbeitung aller vom Receiver eingehenden Pakete eines Links. */
void handle_srtla_data(conn_t *c) {
  char buf[MTU];

  int n = recvfrom(c->fd, &buf, MTU, 0, NULL, NULL);
  if (n <= 0) return;

  time_t ts;
  get_seconds(&ts);

  uint16_t packet_type = get_srt_type(buf, n);

  /* NGP/REG2 separat behandeln, BEVOR wir last_rcvd setzen - sonst koennten
     diese Pakete einen in Wahrheit ausgefallenen Link als aktiv erscheinen
     lassen. */
  if (packet_type == SRTLA_TYPE_REG_NGP) {
    /* NGP nur verarbeiten, wenn:
       * wir keine etablierten Verbindungen haben,
       * kein REG1->REG2-Austausch laeuft,
       * und kein REG2->REG3-Austausch aussteht. */
    if (active_connections == 0 && pending_reg2_conn == NULL && ts > pending_reg_timeout) {
      if (send_reg1(c) == 0) {
        pending_reg2_conn = c;
        pending_reg_timeout = ts + REG2_TIMEOUT;
      }
    }
    return;

  } else if (packet_type == SRTLA_TYPE_REG2) {
    if (pending_reg2_conn == c) {
      char *id = &buf[2];
      // Die ersten SRTLA_ID_LEN/2 Bytes (unsere Haelfte) muessen passen
      if (memcmp(id, srtla_id, SRTLA_ID_LEN/2) != 0) {
        err("%s (%p): got a mismatching ID in SRTLA_REG2\n",
           print_addr(&c->src), c);
        return;
      }

      info("%s (%p): connection group registered\n", print_addr(&c->src), c);
      memcpy(srtla_id, id, SRTLA_ID_LEN);   // vollstaendige ID uebernehmen

      // REG2 ueber alle Links broadcasten, um jeden Link anzumelden
      for (conn_t *i = conns; i != NULL; i = i->next) {
        send_reg2(i);
      }

      pending_reg2_conn = NULL;
      pending_reg_timeout = ts + REG3_TIMEOUT;
    }
    return;
  }

  c->last_rcvd = ts;

  switch(packet_type) {
    case SRT_TYPE_ACK: {
      // Voller ACK: bestaetigt alle Pakete bis last_ack
      uint32_t last_ack = *((uint32_t *)&buf[16]);
      last_ack = be32toh(last_ack);
      register_srt_ack(last_ack);
      break;
    }

    case SRT_TYPE_NAK: {
      // NAK enthaelt Einzel-IDs und/oder Bereiche (oberstes Bit markiert Bereich)
      uint32_t *ids = (uint32_t *)buf;
      for (int i = 4; i < n/4; i++) {
        uint32_t id = be32toh(ids[i]);
        if (id & (1 << 31)) {
          id = id & 0x7FFFFFFF;            // Bereichsanfang
          uint32_t last_id = be32toh(ids[i+1]); // Bereichsende
          /* Schleifenvariable bewusst unsigned, damit sie zum Typ von last_id
             passt (behebt eine Signedness-Warnung). SRT-Sequenznummern sind
             31-bittig, ein Ueberlauf ist hier praktisch ausgeschlossen. */
          for (uint32_t lost = id; lost <= last_id; lost++) {
            register_nak((int32_t)lost);
          }
          i++;                              // das Bereichsende wurde mitverbraucht
        } else {
          register_nak((int32_t)id);
        }
      }
      break;
    }

    // SRTLA-eigene Pakete unten: NICHT an die SRT-Anwendung weiterreichen
    case SRTLA_TYPE_ACK: {
      uint32_t *acks = (uint32_t *)buf;
      for (int i = 1; i < n/4; i++) {
        uint32_t id = be32toh(acks[i]);
        debug("%s (%p): ack %d\n", print_addr(&c->src), c, id);
        register_srtla_ack((int32_t)id);
      }
      return;
    }
    case SRTLA_TYPE_KEEPALIVE:
      debug("%s (%p): got a keepalive\n", print_addr(&c->src), c);
      return; // nicht an SRT weiterreichen

    case SRTLA_TYPE_REG3:
      has_connected = 1;
      active_connections++;
      info("%s (%p): connection established\n", print_addr(&c->src), c);
      return;
  } // switch

  // Normale SRT-Pakete an die lokale SRT-Anwendung weiterreichen
  sendto(listenfd, &buf, n, 0, &srt_addr, addr_len);
}


/* ===========================================================================
   Verbindungs- und Socketverwaltung
   =========================================================================== */
conn_t *conn_find_by_src(struct sockaddr *src) {
  for (conn_t *c = conns; c != NULL; c = c->next) {
    if (memcmp(src, &c->src, sizeof(*src)) == 0) {
      return c;
    }
  }

  return NULL;
}

/* IP-Liste (eine Quell-IP pro Zeile) einlesen und fuer jede neue IP einen
   Link anlegen. Bereits bekannte IPs werden nur "entmarkiert" (siehe
   update_conns / SIGHUP). */
int setup_conns(char *source_ip_file) {
  FILE *config = fopen(source_ip_file, "r");
  if (config == NULL) {
    perror("Failed to open the source ip list file: ");
    exit_help();
  }

  int count = 0;
  char *line = NULL;
  size_t line_len = 0;
  while(getline(&line, &line_len, config) >= 0) {
    char *nl;
    if ((nl = strchr(line, '\n'))) {
      *nl = '\0';
    }

    struct sockaddr src;

    int ret = parse_ip((struct sockaddr_in *)&src, line);
    if (ret == 0) {
      conn_t *c = conn_find_by_src(&src);
      if (c == NULL) {
        conn_t *c = calloc(1, sizeof(conn_t));
        assert(c != NULL);

        c->src = src;
        c->fd = -1;
        c->window = WINDOW_DEF * WINDOW_MULT;

        c->next = conns;
        conns = c;

        count++;

        printf("Added connection via %s (%p)\n", print_addr(&c->src), c);
      } else {
        c->removed = 0;
      }
    }
  }
  if (line) free(line);

  fclose(config);

  return count;
}

/* IP-Liste zur Laufzeit neu einlesen (per SIGHUP angestossen): zuerst alle
   Links als "removed" markieren, dann die Datei neu lesen (vorhandene werden
   entmarkiert), zuletzt alle noch markierten Links abbauen. */
void update_conns(char *source_ip_file) {
  for (conn_t *c = conns; c != NULL; c = c->next) {
    c->removed = 1;
  }

  setup_conns(source_ip_file);

  conn_t **prev = &conns;
  conn_t *next;
  for (conn_t *c = conns; c != NULL; c = next) {
    next = c->next;
    if (c->removed) {
      printf("Removed connection via %s (%p)\n", print_addr(&c->src), c);

      if (c == pending_reg2_conn) {
        pending_reg2_conn = NULL;
      }

      remove_active_fd(c->fd);
      close(c->fd);
      *prev = c->next;
      free(c);
    } else {
      prev = &c->next;
    }
  }
}

/* SIGHUP-Handler: nur ein Flag setzen, die eigentliche Arbeit passiert in der
   Hauptschleife (Signal-Handler sollen so wenig wie moeglich tun). */
void schedule_update_conns(int signal) {
  (void)signal;            // Parameter wird nicht gebraucht (Signatur vorgegeben)
  do_update_conns = 1;
}

/* Einen UDP-Socket fuer einen Link oeffnen und an dessen Quell-IP binden.
   quiet unterdrueckt die Fehlermeldung beim wiederholten Reconnect-Versuch. */
int open_socket(conn_t *c, int quiet) {
  if (c->fd >= 0) {
    remove_active_fd(c->fd);
    close(c->fd);
    c->fd = -1;
  }

  // Socket anlegen (nicht-blockierend)
  int fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
  if (fd < 0) {
    err("Failed to open a socket");
    return -1;
  }

  int bufsize = SEND_BUF_SIZE;
  int ret = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
  if (ret != 0) {
    err("failed to set send buffer size (%d)\n", bufsize);
    goto err;
  }

  // An die Quell-Adresse binden -> dieses Paket verlaesst das jeweilige Modem
  ret = bind(fd, &c->src, sizeof(c->src));
  if (ret != 0) {
    if (!quiet) {
      err("Failed to bind to the source address %s\n", print_addr(&c->src));
    }
    goto err;
  }

  add_active_fd(fd);
  c->fd = fd;

  return 0;

err:
  close(fd);
  return -1;
}

/* Beim Start fuer jeden Link versuchen, einen Socket zu oeffnen/zu binden.
   host/port werden hier (noch) nicht gebraucht - die Signatur bleibt aber
   stabil fuer kuenftige Erweiterungen. */
int open_conns(char *host, char *port) {
  (void)host; (void)port;     // aktuell ungenutzt (behebt -Wunused-parameter)
  int opened = 0;
  for (conn_t *c = conns; c != NULL; c = c->next) {
    if (open_socket(c, 0) == 0) {
      opened++;
    }
  }
  return opened;
}

/* ===========================================================================
   Verbindungs-Housekeeping (Reconnect, Keepalives, Timeouts)
   =========================================================================== */
void set_srtla_addr(struct addrinfo *addr) {
  memcpy(&srtla_addr, addr->ai_addr, addr->ai_addrlen);
  info("Trying to connect to %s...\n", print_addr(&srtla_addr));
}

void send_keepalive(conn_t *c) {
  debug("%s (%p): sending keepalive\n", print_addr(&c->src), c);
  uint16_t pkt = htobe16(SRTLA_TYPE_KEEPALIVE);
  // Ergebnis bewusst ignoriert
  sendto(c->fd, &pkt, sizeof(pkt), 0, &srtla_addr, addr_len);
}

#define HOUSEKEEPING_INT 1000 // ms
void connection_housekeeping() {
  static uint64_t all_failed_at = 0;
  /* Millisekunden, weil wir mit einem Sekundentakt ein zweites REG2 zu schnell
     nach dem ersten senden koennten - je nachdem, wann im Sekundenintervall
     die Ausfuehrung faellt. */
  static uint64_t last_ran = 0;
  uint64_t ms;
  assert(get_ms(&ms) == 0);
  if ((last_ran + HOUSEKEEPING_INT) > ms) return;

  time_t time = (time_t)(ms / 1000);

  active_connections = 0;

  if (pending_reg2_conn && time > pending_reg_timeout) {
    pending_reg2_conn = NULL;
  }

  for (conn_t *c = conns; c != NULL; c = c->next) {
    if (c->fd < 0) {
      open_socket(c, 1);
      continue;
    }

    if (conn_timed_out(c, time)) {
      /* Beim ersten Erkennen des Ausfalls Status zuruecksetzen und melden. */
      if (c->last_rcvd > 0) {
        info("%s (%p): connection failed, attempting to reconnect\n",
             print_addr(&c->src), c);
        c->last_rcvd = 0;
        c->last_sent = 0;
        c->window = WINDOW_MIN * WINDOW_MULT;
        c->in_flight_pkts = 0;
        for (int i = 0; i < PKT_LOG_SZ; i++) {
          c->pkt_log[i] = -1;
        }
      }

      if (pending_reg2_conn == NULL) {
        /* Da der Link auf unserer Seite abgelaufen ist, hat ihn der Receiver
           evtl. schon aufgeraeumt. Lieber neu anmelden (REG2) als Keepalive. */
        send_reg2(c);
      } else if (pending_reg2_conn == c) {
        send_reg1(c);
      }
      continue;
    }

    /* Link, der in den letzten CONN_TIMEOUT Sekunden Daten empfangen hat,
       gilt als aktiv. */
    active_connections++;

    if ((c->last_sent + IDLE_TIME) < time) {
      send_keepalive(c);
    }
  }

  if (active_connections == 0) {
    if (all_failed_at == 0) {
      all_failed_at = ms;
    }

    if (has_connected) {
      err("warning: no available connections\n");
    }

    // Alle Links aus -> nach GLOBAL_TIMEOUT reagieren
    if (ms > (all_failed_at + (GLOBAL_TIMEOUT * 1000))) {
      if (has_connected) {
        err("Failed to re-establish any connections to %s\n",
            print_addr(&srtla_addr));
        exit(EXIT_FAILURE);
      }

      err("Failed to establish any initial connections to %s\n",
          print_addr(&srtla_addr));

      // Naechste aufgeloeste Receiver-Adresse probieren, sonst beenden
      if (addrs->ai_next) {
        addrs = addrs->ai_next;
        set_srtla_addr(addrs);
        all_failed_at = 0;
      } else {
        exit(EXIT_FAILURE);
      }
    }
  } else {
    all_failed_at = 0;
  }

  last_ran = ms;
}

#define ARG_LISTEN_PORT (argv[1])
#define ARG_SRTLA_HOST  (argv[2])
#define ARG_SRTLA_PORT  (argv[3])
#define ARG_IPS_FILE    (argv[4])
int main(int argc, char **argv) {
  if (argc == 2 && strcmp(argv[1], "-v") == 0) {
    printf(VERSION "\n");
    exit(0);
  }
  if (argc != 5) exit_help();

  source_ip_file = ARG_IPS_FILE;
  int conn_count = setup_conns(source_ip_file);
  if (conn_count <= 0) {
    fprintf(stderr, "Failed to parse any IP addresses in %s\n", source_ip_file);
    exit(EXIT_FAILURE);
  }

  struct sockaddr_in listen_addr;

  int port = parse_port(ARG_LISTEN_PORT);
  if (port < 0) exit_help();

  // Zufaellige Gruppen-ID fuer diese Sitzung erzeugen
  FILE *fd = fopen("/dev/urandom", "rb");
  assert(fd != NULL);
  assert(fread(srtla_id, 1, SRTLA_ID_LEN, fd) == SRTLA_ID_LEN);
  fclose(fd);

  FD_ZERO(&active_fds);

  // Lokalen Listener fuer die SRT-Anwendung einrichten
  listen_addr.sin_family = AF_INET;
  listen_addr.sin_addr.s_addr = INADDR_ANY;
  listen_addr.sin_port = htons(port);
  listenfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (listenfd < 0) { 
    perror("socket creation failed"); 
    exit(EXIT_FAILURE); 
  }

  int ret = bind(listenfd, (struct sockaddr *)&listen_addr, sizeof(listen_addr));
  if (ret < 0) { 
    perror("bind failed"); 
    exit(EXIT_FAILURE); 
  }
  add_active_fd(listenfd);

  int connected = open_conns(ARG_SRTLA_HOST, ARG_SRTLA_PORT);
  if (connected < 1) {
    err("Failed to open and bind to any of the IP addresses in %s\n", source_ip_file);
    exit(EXIT_FAILURE);
  }

  // Adresse(n) des Receivers aufloesen
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  ret = getaddrinfo(ARG_SRTLA_HOST, ARG_SRTLA_PORT, &hints, &addrs);
  if (ret != 0) {
    err("Failed to resolve %s: %s\n", ARG_SRTLA_HOST, gai_strerror(ret));
    exit(EXIT_FAILURE);
  }

  set_srtla_addr(addrs);

  // SIGHUP laedt die IP-Liste neu (Modems zur Laufzeit zu-/abschalten)
  signal(SIGHUP, schedule_update_conns);
  signal(SIGPIPE, SIG_IGN);   // ein geschlossener Socket darf uns nicht killen

  int info_int = LOG_PKT_INT;

  while(1) {
    if (do_update_conns) {
      update_conns(source_ip_file);
      do_update_conns = 0;
    }

    connection_housekeeping();

    // Auf lesbare Sockets warten (max. 200 ms, damit Housekeeping regelmaessig laeuft)
    fd_set read_fds = active_fds;
    struct timeval to = {.tv_sec = 0, .tv_usec = 200*1000};
    ret = select(FD_SETSIZE, &read_fds, NULL, NULL, &to);

    if (ret > 0) {
      if (FD_ISSET(listenfd, &read_fds)) {
        handle_srt_data(listenfd);
      }

      for (conn_t *c = conns; c != NULL; c = c->next) {
        if (c->fd >= 0 && FD_ISSET(c->fd, &read_fds)) {
          handle_srtla_data(c);
        }
      }
    } // ret > 0

    // Gelegentlich einen Statusueberblick je Link ausgeben (nur LOG_DEBUG)
    info_int--;
    if (info_int == 0) {
      for (conn_t *c = conns; c != NULL; c = c->next) {
        debug("%s (%p): in flight: %d, window: %d, last_rcvd %ld\n",
              print_addr(&c->src), c, c->in_flight_pkts, c->window, c->last_rcvd);
      }
      info_int = LOG_PKT_INT;
    }
  } // while(1);
}
