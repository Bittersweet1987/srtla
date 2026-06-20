#!/bin/bash
# Angepasstes SRT/SRTLA Relay Installer (basiert auf escaparrac/IRL-relay-SRT-RTMP)
# Ports: SRTLA=8383, SLS SRT-Port=8282, SLS Stats=8181, srtla_rec-Stats=8484
# Streamid-Schema: publish/live/<beliebiger-name> (Publisher) / play/live/<beliebiger-name> (Player)
#
# NEU gegenueber dem urspruenglichen Skript:
#   * Klont den gepflegten Fork  github.com/Bittersweet1987/srtla  (statt BELABOX)
#   * Richtet den optionalen srtla_rec-Stats-Endpoint (Port 8484) ein und
#     fragt dabei interaktiv nach einem Schutz-Key (oder generiert einen).
set -e

SRTLA_PORT=8383
SLS_SRT_PORT=8282
SLS_HTTP_PORT=8181
STATS_PORT=8484                 # NEU: srtla_rec-eigener Stats-Endpoint
STATS_ADDR="0.0.0.0"            # von aussen erreichbar (per Key geschuetzt)

echo "=== SRT/SRTLA Relay Installer (custom ports) ==="
echo "SRTLA-Port: $SRTLA_PORT | SLS SRT-Port: $SLS_SRT_PORT | SLS Stats-Port: $SLS_HTTP_PORT | srtla-Stats: $STATS_PORT"

localip=$(hostname -I | awk '{print $1}')
publicip=$(curl -s -4 ifconfig.me || echo "UNKNOWN")
username=$USER

echo "Checking what type of user you are:"
if [ "$((EUID))" -eq 0 ] && [ -n "$SUDO_USER" ]; then
    username="$SUDO_USER"
    echo "You are running the script as sudo. Username is now set to: $username"
elif [ "$((EUID))" -eq 0 ] ; then
    username="root"
    echo "You are root. Username is now set to: $username"
else
    username="$USER"
    echo "Username is now set to: $username"
fi

USER_HOME="/home/$username"
if [ "$username" = "root" ]; then
    USER_HOME="/root"
fi

# ============================================================================
# NEU: Stats-Endpoint-Key festlegen (interaktiv, mit sicheren Fallbacks)
# ----------------------------------------------------------------------------
# Reihenfolge der Quellen:
#   1. Wird STATS_KEY bereits als Umgebungsvariable uebergeben, diese nutzen.
#   2. Interaktives Terminal -> Nutzer fragen (Auto-Generieren / eigener / aus).
#   3. Kein Terminal (z. B. "curl ... | bash") -> automatisch Key generieren.
# So ist der Endpoint NIE versehentlich ungeschuetzt offen.
# ============================================================================
STATS_ENABLED=1
echo ""
echo "=== srtla_rec Stats-Endpoint (Port $STATS_PORT) ==="
echo "Zeigt die Auslastung pro Modem/Link (Adresse, Paket-/Byte-Zaehler)."
echo "Da er von aussen erreichbar ist, wird er mit einem Key geschuetzt."

if [ -n "${STATS_KEY:-}" ]; then
    echo "STATS_KEY aus der Umgebung uebernommen."
elif [ -t 0 ]; then
    echo ""
    echo "  [1] Key automatisch generieren (empfohlen)"
    echo "  [2] Eigenen Key eingeben"
    echo "  [3] Stats-Endpoint deaktivieren"
    read -r -p "Auswahl [1]: " choice
    choice="${choice:-1}"
    case "$choice" in
        2)
            read -r -p "Eigenen Key eingeben: " STATS_KEY
            if [ -z "$STATS_KEY" ]; then
                STATS_KEY="$(openssl rand -hex 16)"
                echo "Leer -> automatisch generiert."
            fi
            ;;
        3)
            STATS_ENABLED=0
            echo "WARNUNG: Stats-Endpoint wird deaktiviert."
            ;;
        *)
            STATS_KEY="$(openssl rand -hex 16)"
            echo "Key automatisch generiert."
            ;;
    esac
else
    # Nicht-interaktiv (Pipe): sicherer Default = Key generieren
    STATS_KEY="$(openssl rand -hex 16)"
    echo "Nicht-interaktive Installation: Key automatisch generiert."
fi

echo "Updating and upgrading the OS"
sudo apt-get update -y -q
sudo apt-get upgrade -y -q
sudo dpkg --configure -a || true
echo "System updated and upgraded"

echo "Installing all the required packages"
sudo apt-get install libinput-dev make cmake tcl openssl zlib1g-dev gcc perl net-tools nano ssh git zip unzip tclsh pkg-config libssl-dev build-essential iputils-ping ufw -y -q
echo "All packages installed correctly"

echo "Preparing ports to be opened: $SLS_HTTP_PORT(tcp)=Stats, $SLS_SRT_PORT(udp)=SLS/SRT, $SRTLA_PORT(udp)=SRTLA, $STATS_PORT(tcp)=srtla-Stats, 22=SSH"
sudo ufw allow "$SLS_HTTP_PORT"/tcp
sudo ufw allow "$SLS_SRT_PORT"/udp
sudo ufw allow "$SLS_SRT_PORT"/tcp
sudo ufw allow "$SRTLA_PORT"/udp
sudo ufw allow "$SRTLA_PORT"/tcp
if [ "$STATS_ENABLED" -eq 1 ]; then
    sudo ufw allow "$STATS_PORT"/tcp        # NEU: srtla_rec-Stats-Endpoint
fi
sudo ufw allow 22/tcp
sudo ufw allow 22/udp

echo "Enabling the firewall service"
echo "y" | sudo ufw enable
echo "Firewall enabled"

echo "Downloading and installing SRT Server. This can take a few minutes."
cd "$USER_HOME"
if [ ! -d srt ]; then
    sudo git clone https://github.com/Haivision/srt.git -q
fi
cd srt
sudo cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 .
sudo make -j"$(nproc)"
sudo make install
sudo ldconfig
if command -v srt-live-transmit >/dev/null 2>&1; then
    echo "Success: SRT installed."
else
    echo "Error: SRT could not be installed. Stopping the script."
    exit 1
fi
cd "$USER_HOME"
echo "SRT Server correctly installed"

echo "Downloading and installing SLS"
if [ ! -d srt-live-server ]; then
    sudo git clone https://github.com/escaparrac/srt-live-server.git -q
fi
cd srt-live-server
echo "Patching missing <ctime> include for GCC 15+ compatibility"
sudo sed -i '1i #include <ctime>' slscore/common.hpp
sudo make -j"$(nproc)"

echo "Writing custom sls.conf with your ports and flexible streamid scheme"
sudo bash -c "cat > sls.conf" <<EOF
srt {
    worker_threads 1;
    worker_connections 200;
    http_port ${SLS_HTTP_PORT};
    cors_header *;
    log_file /dev/stdout;

    server {
        listen ${SLS_SRT_PORT};
        latency 2000;
        domain_player play;
        domain_publisher publish;
        default_sid play/live/feed1;
        backlog 10;
        idle_streams_timeout 10;

        app {
            app_publisher live;
            app_player live;
        }
    }
}
EOF
echo "sls.conf written"

cd "$USER_HOME"

echo "Creating sls.sh startup script"
sudo bash -c "cat > $USER_HOME/sls.sh" <<EOF
#!/bin/bash
cd ${USER_HOME}/srt-live-server/bin/
./sls -c ../sls.conf
EOF
sudo chmod +x "$USER_HOME/sls.sh"

echo "Creating sls.service"
sudo bash -c "cat > /etc/systemd/system/sls.service" <<EOF
[Unit]
Description=sls

[Service]
ExecStart=/bin/bash ${USER_HOME}/sls.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable sls.service
sudo systemctl restart sls.service
echo "SLS service enabled and started"

echo "Installing SRTLA Relay Server (gepflegter Fork Bittersweet1987/srtla)"
cd "$USER_HOME"
# NEU: gepflegten Fork klonen statt des nicht mehr betreuten BELABOX-Repos.
# Ein evtl. vorhandenes altes 'srtla' wird auf den Fork umgestellt.
if [ ! -d srtla ]; then
    sudo git clone https://github.com/Bittersweet1987/srtla.git -q
else
    cd srtla
    if ! sudo git remote get-url origin 2>/dev/null | grep -q "Bittersweet1987/srtla"; then
        echo "Vorhandenes srtla-Repo -> auf den Fork umstellen"
        sudo git remote set-url origin https://github.com/Bittersweet1987/srtla.git
    fi
    sudo git fetch origin -q || true
    sudo git checkout main -q || true
    sudo git reset --hard origin/main -q || true
    cd "$USER_HOME"
fi
cd srtla
sudo make -j"$(nproc)"
echo "SRTLA Relay Server installed"

cd "$USER_HOME"

# NEU: Konfiguration (inkl. Stats-Key) in eine NUR fuer root lesbare Datei.
# So steht der Key nicht in einem world-readable Skript oder in 'ps'.
echo "Writing /etc/default/srtla_rec (chmod 600)"
if [ "$STATS_ENABLED" -eq 1 ]; then
    EFF_STATS_PORT="$STATS_PORT"
else
    EFF_STATS_PORT=""           # leer => Endpoint im Binary deaktiviert
fi
sudo bash -c "cat > /etc/default/srtla_rec" <<EOF
# Konfiguration fuer den srtla_rec-Dienst (von systemd via EnvironmentFile gelesen)
SRTLA_LISTEN_PORT="${SRTLA_PORT}"
SRT_ADDR="127.0.0.1 ${SLS_SRT_PORT}"
STATS_PORT="${EFF_STATS_PORT}"
STATS_ADDR="${STATS_ADDR}"
STATS_KEY="${STATS_KEY:-}"
EOF
sudo chmod 600 /etc/default/srtla_rec

echo "Creating srtla.sh startup script"
sudo bash -c "cat > $USER_HOME/srtla.sh" <<EOF
#!/bin/bash
cd ${USER_HOME}/srtla
# STATS_PORT/STATS_ADDR/STATS_KEY kommen aus /etc/default/srtla_rec (systemd-Env)
./srtla_rec ${SRTLA_PORT} 127.0.0.1 ${SLS_SRT_PORT}
EOF
sudo chmod +x "$USER_HOME/srtla.sh"

echo "Creating srtla.service"
sudo bash -c "cat > /etc/systemd/system/srtla.service" <<EOF
[Unit]
Description=srtla
After=sls.service

[Service]
EnvironmentFile=/etc/default/srtla_rec
ExecStart=/bin/bash ${USER_HOME}/srtla.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable srtla.service
sudo systemctl restart srtla.service
echo "SRTLA service enabled and started"

echo ""
echo "=================================================="
echo "Installation abgeschlossen!"
echo "=================================================="
echo "SRTLA-Sender verbindet sich auf (beliebiger Name statt feed1 moeglich):"
echo "  srtla://${publicip}:${SRTLA_PORT}?streamid=publish/live/feed1"
echo ""
echo "SRT-Stream abspielen (OBS/VLC):"
echo "  srt://${publicip}:${SLS_SRT_PORT}/?streamid=play/live/feed1"
echo ""
echo "Stats (SLS, gebuendelter Strom):"
echo "  http://${publicip}:${SLS_HTTP_PORT}/stats"
echo ""
if [ "$STATS_ENABLED" -eq 1 ]; then
echo "Stats (srtla_rec, Auslastung pro Modem):"
echo "  http://${publicip}:${STATS_PORT}/?key=${STATS_KEY}"
echo "  >> Diesen Key sicher aufbewahren - er steht in /etc/default/srtla_rec <<"
echo ""
fi
echo "Hinweis: 'feed1' ist frei waehlbar, kann durch jeden beliebigen Namen ersetzt werden,"
echo "ohne dass die Config angepasst werden muss (z.B. feed2, kamera1, x7k29fa, ...)."
echo ""
echo "Status pruefen mit:"
echo "  sudo systemctl status sls.service"
echo "  sudo systemctl status srtla.service"
echo "=================================================="
