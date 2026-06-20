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
   common.c - Implementierung der gemeinsamen Hilfsfunktionen aus common.h.
   Enthaelt nur kleine, zustandslose Helfer (Adress-/Zeit-/Paket-Parsing).
   =========================================================================== */

#include <endian.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#include "common.h"

/* Hilfe ausgeben (print_help() ist je Programm unterschiedlich definiert)
   und den Prozess mit Fehlercode verlassen. */
void exit_help() {
  print_help();
  exit(EXIT_FAILURE);
}

/* print_addr() formatiert eine IPv4-Adresse als Text.
   ACHTUNG: nutzt einen globalen, statischen Puffer und ist daher NICHT
   reentrant/threadsicher - das Ergebnis gilt nur bis zum naechsten Aufruf.
   Das ist hier unkritisch, weil beide Programme single-threaded sind und
   den Rueckgabewert sofort (z. B. in einem printf) verwenden. */
#define ADDR_BUF_SZ 50
char _global_addr_buf[ADDR_BUF_SZ];
const char *print_addr(struct sockaddr *addr) {
  struct sockaddr_in *ain = (struct sockaddr_in *)addr;
  return inet_ntop(ain->sin_family, &ain->sin_addr, _global_addr_buf, ADDR_BUF_SZ);
}

/* Portnummer aus einer sockaddr lesen und von Network- in Host-Byteorder
   umwandeln. */
int port_no(struct sockaddr *addr) {
  struct sockaddr_in *ain = (struct sockaddr_in *)addr;
  return ntohs(ain->sin_port);
}

/* Einen IPv4-String ("1.2.3.4") in eine sockaddr_in schreiben.
   Verbesserung gegenueber dem Original: inet_pton() statt inet_addr().
   inet_addr() liefert (in_addr_t)-1 sowohl bei Fehler als auch fuer die
   gueltige Adresse 255.255.255.255 und erzeugte zudem eine Signedness-
   Warnung. inet_pton() ist eindeutig (Rueckgabe 1 = ok, 0 = ungueltig,
   -1 = falsche Familie) und die empfohlene moderne API. */
int parse_ip(struct sockaddr_in *addr, char *ip_str) {
  memset(addr, 0, sizeof(*addr));
  addr->sin_family = AF_INET;

  /* inet_pton liefert genau 1 bei erfolgreicher Umwandlung. */
  if (inet_pton(AF_INET, ip_str, &addr->sin_addr) != 1) return -1;

  return 0;
}

/* Portstring robust parsen.
   Verbesserung: Wir pruefen jetzt auch auf Muellzeichen am Ende
   (z. B. "8282abc") und auf einen leeren String, indem wir den
   endptr von strtol auswerten. Gueltiger Bereich: 1..65535. */
int parse_port(char *port_str) {
  if (port_str == NULL || *port_str == '\0') return -1;
  char *endptr = NULL;
  long port = strtol(port_str, &endptr, 10);
  if (*endptr != '\0') return -1;          // unerwartete Zeichen nach der Zahl
  if (port <= 0 || port > 65535) return -2;
  return (int)port;
}

/* Monotone Uhr in Sekunden. CLOCK_MONOTONIC_COARSE ist sehr guenstig
   (kein Syscall in den Kernel-Vsdo-faellen) und genau genug fuer unsere
   Timeouts im Sekundenbereich. Monoton = laeuft nicht rueckwaerts, auch
   nicht bei NTP-Korrekturen der Systemzeit. */
int get_seconds(time_t *s) {
  struct timespec ts;
  int ret = clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  if (ret != 0) return -1;
  *s = ts.tv_sec;
  return 0;
}

/* Wie get_seconds(), aber in Millisekunden - wird im Sender fuer das
   feinere Housekeeping-Intervall gebraucht. */
int get_ms(uint64_t *ms) {
  struct timespec ts;
  int ret = clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  if (ret != 0) return -1;
  *ms = ((uint64_t)(ts.tv_sec)) * 1000 + ((uint64_t)(ts.tv_nsec)) / 1000 / 1000;

  return 0;
}

/* SRT-Sequenznummer aus einem Datenpaket lesen.
   Im SRT-Wire-Format unterscheidet das oberste Bit (Bit 31) zwischen
   Daten- (0) und Control-Paketen (1). Bei einem Control-Paket gibt es keine
   Sequenznummer -> Rueckgabe -1. */
int32_t get_srt_sn(void *pkt, int n) {
  if (n < 4) return -1;

  uint32_t sn = be32toh(*((uint32_t *)pkt));
  if ((sn & (1 << 31)) == 0) {
    return (int32_t)sn;
  }

  return -1;
}

/* Ersten 16-Bit-Wert (Pakettyp) big-endian aus dem Paket lesen. */
uint16_t get_srt_type(void *pkt, int n) {
  if (n < 2) return 0;
  return be16toh(*((uint16_t *)pkt));
}

/* Bequeme Typ-Praedikate. Bei den SRTLA_REG*-Varianten pruefen wir
   zusaetzlich die exakte Paketlaenge, damit kein fremdes Paket
   versehentlich als Registrierung interpretiert wird. */
int is_srt_ack(void *pkt, int n) {
  return get_srt_type(pkt, n) == SRT_TYPE_ACK;
}

int is_srtla_keepalive(void *pkt, int n) {
  return get_srt_type(pkt, n) == SRTLA_TYPE_KEEPALIVE;
}

int is_srtla_reg1(void *pkt, int len) {
  if (len != SRTLA_TYPE_REG1_LEN) return 0;
  return get_srt_type(pkt, len) == SRTLA_TYPE_REG1;
}

int is_srtla_reg2(void *pkt, int len) {
  if (len != SRTLA_TYPE_REG2_LEN) return 0;
  return get_srt_type(pkt, len) == SRTLA_TYPE_REG2;
}

int is_srtla_reg3(void *pkt, int len) {
  if (len != SRTLA_TYPE_REG3_LEN) return 0;
  return get_srt_type(pkt, len) == SRTLA_TYPE_REG3;
}
