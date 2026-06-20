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
   common.h - Gemeinsame Definitionen fuer srtla_send (Sender) und
              srtla_rec (Receiver).

   WICHTIG: Alles in diesem Header beschreibt das SRTLA-/SRT-"Wire-Format",
   also die exakte Byte-Anordnung der Pakete, die ueber das Netz gehen.
   Diese Konstanten und Strukturen duerfen NICHT veraendert werden, sonst
   ist die Kompatibilitaet zu bestehenden Sendern (Belabox, Moblin, IRL Pro)
   und Receivern verloren. Kommentare und Tooling drumherum sind frei
   aenderbar - das Protokoll selbst ist es nicht.

   Kurz zum Hintergrund (fuer das Verstaendnis des restlichen Codes):
   - SRT ist ein verbindungsorientiertes UDP-Transportprotokoll fuer Video
     mit geringer Latenz. SRTLA legt sich *zwischen* die SRT-Anwendung und
     das Netz und verteilt die einzelnen UDP-Pakete eines SRT-Streams auf
     mehrere Netzwerk-Links (z. B. mehrere LTE-Modems) -> "Link Aggregation".
   - Der Sender (srtla_send) buendelt, der Receiver (srtla_rec) entbuendelt
     und reicht den wieder zusammengesetzten SRT-Strom an einen echten
     SRT-Server (hier: SLS / srt-live-server) weiter.
   =========================================================================== */

/* Maximale UDP-Nutzlast, die wir je Paket erwarten/lesen. 1500 = klassische
   Ethernet-MTU. Alle Lese-Puffer sind MTU gross. */
#define MTU 1500

/* ---------------------------------------------------------------------------
   SRT-Steuerpakettypen (oberstes Bit gesetzt => Control-Paket).
   Werte stammen aus der SRT-Spezifikation und werden im Header big-endian
   uebertragen. Wir werten nur die Typen aus, die SRTLA fuer die
   Link-Steuerung braucht (Handshake/ACK/NAK/Shutdown).
   --------------------------------------------------------------------------- */
#define SRT_TYPE_HANDSHAKE   0x8000  // Verbindungsaufbau (Induction/Conclusion)
#define SRT_TYPE_ACK         0x8002  // Empfangsbestaetigung des SRT-Empfaengers
#define SRT_TYPE_NAK         0x8003  // Verlustmeldung (Negative ACK)
#define SRT_TYPE_SHUTDOWN    0x8005  // Geordneter Verbindungsabbau

/* ---------------------------------------------------------------------------
   SRTLA-eigene Pakettypen. Diese gibt es nur zwischen srtla_send und
   srtla_rec; ein normaler SRT-Server sieht sie nie.
   REG1/REG2/REG3 bilden den 2-Phasen-Registrierungs-Handshake, mit dem
   mehrere Links zu einer gemeinsamen "Connection Group" zusammengefasst
   werden (srtla v2 / "srtla2").
   --------------------------------------------------------------------------- */
#define SRTLA_TYPE_KEEPALIVE 0x9000  // Link am Leben halten / RTT messen
#define SRTLA_TYPE_ACK       0x9100  // SRTLA-eigene ACK (pro Link, fuer Balancing)
#define SRTLA_TYPE_REG1      0x9200  // Sender->Receiver: Gruppe anlegen (Phase 1)
#define SRTLA_TYPE_REG2      0x9201  // Beidseitig: Gruppen-ID bestaetigen (Phase 2)
#define SRTLA_TYPE_REG3      0x9202  // Receiver->Sender: Link aufgenommen (Phase 3)
#define SRTLA_TYPE_REG_ERR   0x9210  // Voruebergehender Fehler, spaeter erneut versuchen
#define SRTLA_TYPE_REG_NGP   0x9211  // "No Group": unbekannte Gruppen-ID -> neu mit REG1
#define SRTLA_TYPE_REG_NAK   0x9212  // Endgueltige Ablehnung (inkompatibel/verweigert)

/* Kleinste Laenge, die ein gueltiges SRT-Paket haben kann (Header). Kuerzere
   Pakete werden verworfen. */
#define SRT_MIN_LEN          16

/* Laenge der SRTLA-Gruppen-ID in Bytes. Die ID wird zur Haelfte vom Sender
   und zur Haelfte vom Receiver erzeugt (siehe REG1/REG2-Ablauf im README). */
#define SRTLA_ID_LEN         256
#define SRTLA_TYPE_REG1_LEN  (2 + (SRTLA_ID_LEN))  // 2 Byte Typ + ID
#define SRTLA_TYPE_REG2_LEN  (2 + (SRTLA_ID_LEN))  // 2 Byte Typ + ID
#define SRTLA_TYPE_REG3_LEN  2                      // nur 2 Byte Typ

/* SRT-Paketkopf (Common Header). __packed__, damit der Compiler keine
   Ausrichtungs-Luecken einfuegt und das Layout exakt dem Wire-Format
   entspricht. Alle Mehrbyte-Felder liegen big-endian vor. */
typedef struct __attribute__((__packed__)) {
  uint16_t type;       // Pakettyp (z. B. SRT_TYPE_ACK)
  uint16_t subtype;    // Untertyp / reserviert je nach Typ
  uint32_t info;       // typabhaengige Zusatzinfo
  uint32_t timestamp;  // SRT-Zeitstempel
  uint32_t dest_id;    // Ziel-Socket-ID
} srt_header_t;

/* SRT-Handshake-Paket (Induction). Wird vom Receiver benutzt, um beim Start
   zu pruefen, ob hinter SRT_HOST:SRT_PORT ueberhaupt ein SRT-Server antwortet
   (siehe resolve_srt_addr() in srtla_rec.c). */
typedef struct __attribute__((__packed__)) {
  srt_header_t header;
  uint32_t version;         // SRT-Version (hier 4 fuer Induction)
  uint16_t enc_field;       // Verschluesselungsfeld
  uint16_t ext_field;       // Erweiterungsfeld
  uint32_t initial_seq;     // Start-Sequenznummer
  uint32_t mtu;             // ausgehandelte MTU
  uint32_t mfw;             // max. Flow-Window
  uint32_t handshake_type;  // Handshake-Phase
  uint32_t source_id;       // eigene Socket-ID
  uint32_t syn_cookie;      // Cookie gegen Spoofing
  char     peer_ip[16];     // Adresse der Gegenstelle
} srt_handshake_t;

/* ---------------------------------------------------------------------------
   Log-Level. Zur Compile-Zeit waehlbar - die Makros err()/info()/debug()
   verschwinden komplett, wenn das Level zu niedrig ist (kein Laufzeit-Overhead).
   Hinweis: Fuer die Fehlersuche auf dem VPS lohnt es sich, hier kurzzeitig
   LOG_DEBUG zu setzen und neu zu bauen.
   --------------------------------------------------------------------------- */
#define LOG_NONE    0   // nur fatale Fehler
#define LOG_ERR     1   // Fehler, die wir tolerieren koennen
#define LOG_INFO    2   // informative Meldungen
#define LOG_DEBUG   3   // sehr ausfuehrlich, interne Ablaeufe

#define LOG_LEVEL LOG_INFO

#if LOG_LEVEL >= LOG_DEBUG
  #define debug(...) fprintf(stderr, __VA_ARGS__)
#else
  #define debug(...)
#endif

#if LOG_LEVEL >= LOG_INFO
  #define info(...) fprintf(stderr, __VA_ARGS__)
#else
  #define info(...)
#endif

#if LOG_LEVEL >= LOG_ERR
  #define err(...) fprintf(stderr, __VA_ARGS__)
#else
  #define err(...)
#endif

/* ---------------------------------------------------------------------------
   Gemeinsame Hilfsfunktionen (Implementierung in common.c).
   --------------------------------------------------------------------------- */
void print_help();   // wird je Programm passend definiert (Sender/Receiver)
void exit_help();     // Hilfe ausgeben und mit Fehlercode beenden

int get_seconds(time_t *s);   // monotone Zeit in Sekunden
int get_ms(uint64_t *ms);     // monotone Zeit in Millisekunden

const char *print_addr(struct sockaddr *addr);  // IP als Text (nicht reentrant!)
int port_no(struct sockaddr *addr);             // Portnummer aus sockaddr
int parse_ip(struct sockaddr_in *addr, char *ip_str);  // "1.2.3.4" -> sockaddr_in
int parse_port(char *port_str);                 // Portstring -> 1..65535 oder <0

int32_t get_srt_sn(void *pkt, int n);     // SRT-Sequenznummer (oder -1 bei Control)
uint16_t get_srt_type(void *pkt, int n);  // SRT-/SRTLA-Pakettyp
int is_srt_ack(void *pkt, int n);
int is_srt_shutdown(void *pkt, int n);

int is_srtla_keepalive(void *pkt, int len);
int is_srtla_reg1(void *pkt, int len);
int is_srtla_reg2(void *pkt, int len);
int is_srtla_reg3(void *pkt, int len);
