#!/bin/bash
# ============================================================================
# install.sh - srtla_rec als systemd-Dienst installieren.
#
# Schritte:
#   1. srtla_rec bauen (falls noch nicht geschehen)
#   2. Binary nach /usr/local/bin kopieren
#   3. Default-/Konfigurationsdatei nach /etc/default kopieren (nicht ueberschreiben)
#   4. systemd-Unit installieren, aktivieren und starten
#
# Als root bzw. mit sudo ausfuehren.
# ============================================================================
set -euo pipefail

# Verzeichnis dieses Skripts (Repo-Wurzel ist eine Ebene darueber)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$(id -u)" -ne 0 ]; then
  echo "Bitte als root oder mit sudo ausfuehren." >&2
  exit 1
fi

# 1. Bauen, falls das Binary fehlt
if [ ! -x "$REPO_DIR/srtla_rec" ]; then
  echo "==> Baue srtla_rec ..."
  make -C "$REPO_DIR" srtla_rec
fi

# 2. Binary installieren
echo "==> Installiere /usr/local/bin/srtla_rec"
install -m 0755 "$REPO_DIR/srtla_rec" /usr/local/bin/srtla_rec

# 3. Konfiguration installieren (bestehende Datei NICHT ueberschreiben)
if [ ! -f /etc/default/srtla_rec ]; then
  echo "==> Installiere /etc/default/srtla_rec"
  install -m 0644 "$SCRIPT_DIR/srtla_rec_default" /etc/default/srtla_rec
else
  echo "==> /etc/default/srtla_rec existiert bereits - bleibt unveraendert"
fi

# 4. systemd-Unit installieren und aktivieren
echo "==> Installiere systemd-Unit"
install -m 0644 "$SCRIPT_DIR/srtla_rec.service" /etc/systemd/system/srtla_rec.service
systemctl daemon-reload
systemctl enable --now srtla_rec.service

echo "==> Fertig. Status:"
systemctl --no-pager --full status srtla_rec.service || true
