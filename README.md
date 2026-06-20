# srtla – SRT Relay mit Link-Bündelung / SRT relay with link bonding

🇩🇪 **Deutsch:** Mit diesem Projekt baust du deinen eigenen Server für mobiles
Live-Streaming (IRL). Er bündelt mehrere Internet-Verbindungen (z. B. mehrere
Handy-SIMs/Modems) zu einem stabilen Stream – ideal für BelaBox/IRLBox oder die
Handy-Apps **Moblin** (iOS) und **IRL Pro** (Android).

🇬🇧 **English:** This project lets you run your own server for mobile live
streaming (IRL). It bonds several internet connections (e.g. multiple phone
SIMs/modems) into one stable stream – ideal for BelaBox/IRLBox or the phone
apps **Moblin** (iOS) and **IRL Pro** (Android).

> Dies ist ein gepflegter Fork von [BELABOX/srtla](https://github.com/BELABOX/srtla)
> mit Fehlerbehebungen, Härtung und einem Statistik-Endpoint. Details:
> [CHANGELOG.md](CHANGELOG.md). · *Maintained fork of BELABOX/srtla with bug
> fixes, hardening and a stats endpoint. Details in [CHANGELOG.md](CHANGELOG.md).*

---

## 🚀 Installation (einfach) / Installation (easy)

🇩🇪 **Du brauchst:** einen Server („VPS") mit **Ubuntu 24.04** oder **26.04** und
deine Server-IP-Adresse. Den VPS mietest du bei einem Anbieter deiner Wahl.

🇬🇧 **You need:** a server ("VPS") running **Ubuntu 24.04** or **26.04** and your
server's IP address. Rent the VPS from any provider you like.

### Schritt 1 – Mit dem Server verbinden / Connect to the server

🇩🇪 Verbinde dich per SSH mit deinem Server.
Windows: „Terminal" oder „PowerShell" öffnen. Mac/Linux: „Terminal" öffnen.

🇬🇧 Connect to your server via SSH.
Windows: open "Terminal" or "PowerShell". Mac/Linux: open "Terminal".

```bash
ssh root@DEINE_SERVER_IP
```

### Schritt 2 – Welches Ubuntu? / Which Ubuntu?

🇩🇪 Nicht sicher? Dieser Befehl zeigt deine Version an:
🇬🇧 Not sure? This command shows your version:

```bash
lsb_release -a
```

### Schritt 3 – Installer ausführen / Run the installer

🇩🇪 Kopiere **einen** der folgenden Blöcke (passend zu deiner Ubuntu-Version) und
füge ihn ins Terminal ein. Der Rest läuft automatisch.

🇬🇧 Copy **one** of the blocks below (matching your Ubuntu version) and paste it
into the terminal. Everything else runs automatically.

**Ubuntu 24.04**
```bash
curl -fsSL -o install.sh https://raw.githubusercontent.com/Bittersweet1987/srtla/main/install/install-srtla-sls-ubuntu-2404.sh
chmod +x install.sh && ./install.sh
```

**Ubuntu 26.04**
```bash
curl -fsSL -o install.sh https://raw.githubusercontent.com/Bittersweet1987/srtla/main/install/install-srtla-sls-ubuntu-2604.sh
chmod +x install.sh && ./install.sh
```

### Schritt 4 – Fragen beantworten / Answer the questions

🇩🇪 Während der Installation wirst du nach einem **Schutz-Schlüssel (Key)** für
die Statistik-Seite gefragt. Im Zweifel einfach **Enter** drücken – dann wird
automatisch ein sicherer Schlüssel erzeugt und am Ende angezeigt. **Schreib ihn
dir auf.**

🇬🇧 During installation you'll be asked for a **protection key** for the stats
page. If unsure, just press **Enter** – a secure key is generated automatically
and shown at the end. **Write it down.**

---

## ✅ Fertig – was du bekommst / Done – what you get

🇩🇪 Am Ende zeigt der Installer dir deine fertigen Adressen. So sehen sie aus
(`feed1` ist frei wählbar, `DEINE_IP` = deine Server-IP):

🇬🇧 At the end the installer shows your ready-to-use addresses. They look like
this (`feed1` is freely choosable, `DEINE_IP` = your server IP):

| | URL |
|---|---|
| 🎥 **Senden / Send** (BelaBox, Moblin, IRL Pro) | `srtla://DEINE_IP:8383?streamid=publish/live/feed1` |
| ▶️ **Abspielen / Play** (OBS, VLC) | `srt://DEINE_IP:8282/?streamid=play/live/feed1` |
| 📊 **Statistik / Stats** (pro Modem) | `http://DEINE_IP:8484/?key=DEIN_KEY` |

---

## 📊 Statistik ansehen / View statistics

🇩🇪 Die Statistik-Seite zeigt, **wie viel jedes Modem gerade trägt** – sehr
praktisch, um beim Streamen unterwegs zu sehen, ob alle Verbindungen aktiv sind.
Einfach die Adresse oben im Browser (auch am Handy) öffnen.

🇬🇧 The stats page shows **how much each modem is currently carrying** – very
handy while streaming on the go to check that all links are active. Just open
the address above in a browser (works on your phone too).

> 🔒 🇩🇪 Der Key schützt deine Statistik. Ohne richtigen Key gibt es keine Daten.
> 🇬🇧 The key protects your stats. Without the correct key, no data is shown.

---

## 🛠️ Server verwalten / Manage the server

🇩🇪 Status prüfen, neu starten, Log ansehen:
🇬🇧 Check status, restart, view logs:

```bash
sudo systemctl status srtla.service     # läuft es? / is it running?
sudo systemctl restart srtla.service    # neu starten / restart
journalctl -u srtla.service -f          # Live-Log ansehen / watch live log
```

🇩🇪 Schlüssel/Ports ändern: Datei `/etc/default/srtla_rec` bearbeiten, dann
`sudo systemctl restart srtla.service`.
🇬🇧 Change key/ports: edit `/etc/default/srtla_rec`, then
`sudo systemctl restart srtla.service`.

---

## 🔄 Aktualisieren / Update

🇩🇪 Einfach den Installer aus Schritt 3 erneut ausführen – er holt die neueste
Version. *Achtung:* ein vorhandener `srtla`-Ordner auf dem Server wird dabei auf
den neuesten Stand zurückgesetzt.

🇬🇧 Just run the installer from step 3 again – it pulls the latest version.
*Note:* an existing `srtla` folder on the server is reset to the latest version.

---

## 👩‍💻 Für Entwickler / For developers

🇩🇪 Nur die srtla-Programme selbst bauen (ohne Installer):
🇬🇧 Build just the srtla programs (without the installer):

```bash
git clone https://github.com/Bittersweet1987/srtla.git
cd srtla
make
```

🇩🇪 Erzeugt `srtla_send` (Sender-Seite) und `srtla_rec` (Server/Empfänger). Der
Quellcode ist vollständig auf Deutsch kommentiert. Änderungen: [CHANGELOG.md](CHANGELOG.md).

🇬🇧 Produces `srtla_send` (sender side) and `srtla_rec` (server/receiver). The
source code is fully commented (in German). Changes: [CHANGELOG.md](CHANGELOG.md).

Optionaler Stats-Endpoint / optional stats endpoint (`srtla_rec`):

| Variable | 🇩🇪 | 🇬🇧 |
|---|---|---|
| `STATS_PORT` | TCP-Port, leer = aus | TCP port, empty = off |
| `STATS_ADDR` | Bind-Adresse (Standard `127.0.0.1`) | bind address (default `127.0.0.1`) |
| `STATS_KEY` | Pflicht-Key, leer = offen | required key, empty = open |

---

## ⚠️ Sicherheit / Security

🇩🇪 Wird die Statistik nach außen geöffnet (`STATS_ADDR=0.0.0.0`), **immer einen
Key setzen.** Für „richtig öffentlich" empfiehlt sich HTTPS über einen
Reverse-Proxy (Caddy/nginx) oder ein privates Netz wie Tailscale.

🇬🇧 If you expose the stats to the internet (`STATS_ADDR=0.0.0.0`), **always set a
key.** For truly public access, use HTTPS via a reverse proxy (Caddy/nginx) or a
private network like Tailscale.

---

## 📄 Lizenz / License

🇩🇪 AGPL-3.0 – siehe [LICENSE](LICENSE). · 🇬🇧 AGPL-3.0 – see [LICENSE](LICENSE).
