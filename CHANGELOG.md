# Changelog – Bittersweet1987/srtla Fork

Dieser Fork setzt auf dem letzten Belabox-Stand (`37862da`) auf, den der
ursprüngliche Entwickler nicht mehr weiterpflegt. Ziel: den Code wartbar,
nachvollziehbar (deutsche Kommentare) und robuster machen – **ohne das
Wire-Format zu verändern**, damit bestehende Sender (Belabox, Moblin,
IRL Pro) weiter kompatibel bleiben.

## [Unreleased] – Wartungs- und Härtungsrunde

### Behoben (Bugfixes)
- **`srtla_rec.c` / `conn_reg()`: Use-after-free im Fehlerpfad.**
  Wenn ein **bereits registrierter** Link erneut ein REG2 schickte (passiert
  bei Reconnects) und das anschließende REG3-`sendto` fehlschlug, hängte der
  Fehlerpfad `g->conns = c->next; free(c);` einen noch lebenden Link aus –
  auch wenn dieser nicht der Listenkopf war. Folge: korrupte Liste und
  Freigabe eines benutzten Objekts. Jetzt wird ein Link im Fehlerfall nur
  noch entfernt, wenn er in genau diesem Aufruf **neu** angelegt wurde
  (`newly_allocated`).
- **`common.c` / `parse_ip()`: `inet_addr()` → `inet_pton()`.**
  Beseitigt die Mehrdeutigkeit von `inet_addr()` (Rückgabe `-1` sowohl bei
  Fehler als auch für die gültige Adresse `255.255.255.255`) und die
  Signedness-Warnung.
- **`resolve_srt_addr()`: Ressourcen-Leaks in Fehlerpfaden** (Socket /
  `addrinfo`) geschlossen.

### Hinzugefügt (Robustheit / Betrieb)
- **Socket-Tuning im Receiver:** `SO_REUSEADDR` (sofortiges Neu-Binden nach
  Neustart) sowie 8 MB `SO_RCVBUF`/`SO_SNDBUF` auf dem SRTLA-Socket, um
  Paketverluste bei hoher Bitrate zu reduzieren. Der Sender setzte bereits
  8 MB Sendepuffer – der Receiver zieht nach. (Best-effort, siehe
  sysctl-Hinweis im README.)
- **Geordnetes Herunterfahren** des Receivers bei `SIGTERM`/`SIGINT`: alle
  Gruppen werden abgebaut, Sockets geschlossen (sauberere Logs, valgrind-tauglich).
- **`SIGPIPE` wird ignoriert** (geschlossene Sockets können den Prozess nicht
  mehr beenden), `EINTR` in der epoll-Schleife wird sauber behandelt.
- **`parse_port()`** erkennt jetzt Müllzeichen nach der Zahl (z. B. `8282abc`)
  und leere Eingaben.

### Geändert (Build / Qualität)
- **Gehärteter `Makefile`:** `-Wextra`, `-fstack-protector-strong`,
  `-D_FORTIFY_SOURCE=2`, `-Wformat -Wformat-security`, Linker-Härtung
  (`relro`/`now`). Versions-Fallback auf `unknown`, falls ohne git gebaut wird.
- **`-Wextra`-Warnungen bereinigt:** ungenutzte Parameter
  (`schedule_update_conns`, `open_conns`) und Signedness in der NAK-Schleife
  (`srtla_send.c`).
- **`const_time_cmp()`** auf `unsigned char`-XOR umgestellt (klarer, weiterhin
  konstant-zeitig).

### Dokumentation
- **Gesamter C-Code vollständig auf Deutsch kommentiert** (`common.h`,
  `common.c`, `srtla_rec.c`, `srtla_send.c`): SRTLA-Protokoll, Gruppen-/
  Link-Modell, epoll-Schleife, Fenster-/ACK-/NAK-Logik, Registrierungs-
  Statemachine.
- **systemd-Unit gehärtet und kommentiert** (`ProtectSystem=strict`,
  `PrivateTmp`/`PrivateDevices`, `RestrictAddressFamilies`,
  `MemoryDenyWriteExecute`, automatischer Neustart …).
- **`install.sh`** kommentiert und robuster (`set -euo pipefail`, baut bei
  Bedarf selbst, überschreibt vorhandene Konfiguration nicht).
- README um einen deutschen Fork-Abschnitt inkl. **sysctl-Tuning-Hinweis**
  für die größeren Socket-Puffer ergänzt.

### Ausdrücklich NICHT geändert
- Das **SRT-/SRTLA-Wire-Format** (Pakettypen, Längen, Byte-Reihenfolge,
  Strukturlayouts in `common.h`). Voll kompatibel zum bestehenden Belabox-Stand.
