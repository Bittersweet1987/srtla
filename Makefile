# ============================================================================
# Makefile fuer srtla (srtla_send + srtla_rec)
#
# Aenderungen gegenueber dem Original:
#  - Versions-Fallback: baut auch ausserhalb eines git-Checkouts (z. B. aus
#    einem entpackten Tarball), ohne dass "git rev-parse" fehlschlaegt.
#  - Haertere Compiler-Flags: -Wextra plus gaengige Schutzmechanismen
#    (Stack-Protector, FORTIFY_SOURCE, Format-Warnungen). Das faengt mehr
#    Fehler zur Compile-Zeit ab und erschwert Speicher-Exploits.
#  - .PHONY-Ziele, damit "make clean" auch dann laeuft, wenn eine Datei
#    namens "clean" existiert.
#
# Hinweis: Das auf dem Netz uebertragene Protokoll wird durch diese Flags
# NICHT veraendert - es geht rein um Build-Qualitaet und Sicherheit.
# ============================================================================

# Kurzer git-Commit-Hash als Version; faellt auf "unknown" zurueck, wenn kein
# git verfuegbar ist (z. B. beim Bauen aus einem entpackten Archiv).
VERSION := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# -O2 ist Voraussetzung dafuer, dass _FORTIFY_SOURCE ueberhaupt greift.
CFLAGS := -g -O2 -std=gnu11 \
          -Wall -Wextra \
          -fstack-protector-strong \
          -D_FORTIFY_SOURCE=2 \
          -Wformat -Wformat-security \
          -DVERSION=\"$(VERSION)\"

# Linker-Haertung (nicht-ausfuehrbarer Stack, sofortiges Binding etc.).
LDFLAGS := -Wl,-z,relro,-z,now

all: srtla_send srtla_rec

# Beide Binaries teilen sich common.o (die gemeinsamen Hilfsfunktionen).
srtla_send: srtla_send.o common.o
srtla_rec: srtla_rec.o common.o

clean:
	rm -f *.o srtla_send srtla_rec

.PHONY: all clean
