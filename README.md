Bittersweet1987/srtla – gepflegter Fork
=======================================

Dies ist ein **gewarteter Fork** von [BELABOX/srtla](https://github.com/BELABOX/srtla).
Der ursprüngliche Entwickler pflegt insbesondere den Server-Teil `srtla_rec`
nicht mehr weiter („unsupported, no longer under development"). Dieser Fork
führt die Wartung fort – mit dem Fokus auf **Nachvollziehbarkeit** (der
gesamte C-Code ist auf Deutsch kommentiert), **Robustheit** und **Build-Härtung**.

> **Wichtig:** Das SRT-/SRTLA-Wire-Format wurde **nicht** verändert. Dieser
> Fork bleibt voll kompatibel zu bestehenden Sendern (Belabox, Moblin, IRL Pro)
> und zum unveränderten `srtla_send`. Geändert wurden nur interne Robustheit,
> Kommentare, Build und Deployment.

Was ist neu?
------------

Eine vollständige Liste steht in [`CHANGELOG.md`](CHANGELOG.md). Kurzfassung:

* **Bugfix** in `srtla_rec.c` (`conn_reg`): Use-after-free im Fehlerpfad bei
  Reconnects behoben.
* **Socket-Tuning** im Receiver: `SO_REUSEADDR` + 8 MB Empfangs-/Sendepuffer
  gegen Paketverluste bei hoher Bitrate.
* **Sauberes Herunterfahren** (SIGTERM/SIGINT), `SIGPIPE`-Ignorieren,
  `EINTR`-Behandlung.
* **`inet_pton`** statt `inet_addr`, robusteres Port-Parsing.
* **Gehärteter Build** (`-Wextra`, Stack-Protector, FORTIFY_SOURCE, RELRO/NOW)
  – baut mit **0 Warnungen**.
* **Gehärtete, kommentierte systemd-Unit** und kommentiertes `install.sh`.
* **Gesamter Code auf Deutsch kommentiert.**

Komplett-Installer (SRT + SLS + SRTLA) für einen frischen VPS
-------------------------------------------------------------

Im Ordner [`install/`](install/) liegen fertige Installer für einen frischen
Ubuntu-VPS. Sie bauen SRT (Haivision), SLS (srt-live-server) und diesen
srtla-Fork, richten die systemd-Dienste + Firewall ein und fragen interaktiv
nach einem Key für den srtla_rec-Stats-Endpoint.

    # Ubuntu 24.04
    curl -fsSL -o install.sh https://raw.githubusercontent.com/Bittersweet1987/srtla/main/install/install-srtla-sls-ubuntu-2404.sh
    chmod +x install.sh && ./install.sh

    # Ubuntu 26.04
    curl -fsSL -o install.sh https://raw.githubusercontent.com/Bittersweet1987/srtla/main/install/install-srtla-sls-ubuntu-2604.sh
    chmod +x install.sh && ./install.sh

Erst herunterladen, dann starten – so funktioniert die interaktive Key-Abfrage.
Bei `curl … | bash` (direkt durchgepipt) wird automatisch ein zufälliger Key
erzeugt, weil dann keine Eingabe möglich ist.

Bauen (nur srtla)
-----------------

    git clone https://github.com/Bittersweet1987/srtla.git
    cd srtla
    make

Erzeugt die zwei Programme `srtla_send` und `srtla_rec`.

Als Dienst installieren (Receiver/VPS)
--------------------------------------

    sudo deploy_srtla_rec/install.sh

Das Skript baut bei Bedarf `srtla_rec`, kopiert es nach `/usr/local/bin`,
legt `/etc/default/srtla_rec` an (Ports: SRTLA `8383`, SLS/SRT `8282`) und
installiert/aktiviert die systemd-Unit. Eine vorhandene Konfiguration wird
nicht überschrieben.

Betriebs-Tuning: größere Kernel-Puffer (wichtig!)
-------------------------------------------------

Der Receiver fordert jetzt 8 MB Socket-Puffer an. Als unprivilegierter Dienst
(User `nobody`) deckelt der Kernel diesen Wert aber auf `net.core.rmem_max` /
`net.core.wmem_max`. Damit das Tuning voll greift, diese sysctls dauerhaft
anheben:

    # /etc/sysctl.d/99-srtla.conf
    net.core.rmem_max = 8388608
    net.core.wmem_max = 8388608

Anwenden mit:

    sudo sysctl --system

Stats-/Status-Endpoint (optional)
---------------------------------

`srtla_rec` kann einen kleinen HTTP-Endpoint bereitstellen, der die
**Auslastung pro Link/Modem** zeigt – also genau die Sicht, die SLS' 8181er
Statistik nicht liefert (die kennt nur den bereits gebündelten Strom). Er wird
ausschließlich über Umgebungsvariablen konfiguriert:

| Variable     | Bedeutung                                                            |
|--------------|---------------------------------------------------------------------|
| `STATS_PORT` | TCP-Port. Nicht gesetzt/leer = Endpoint **aus**.                    |
| `STATS_ADDR` | Bind-Adresse. Default `127.0.0.1`. `0.0.0.0` = von außen erreichbar. |
| `STATS_KEY`  | Pflicht-Key. Abruf dann nur mit `?key=…`. Leer = offen (wie 8181).  |

Abruf (JSON):

    curl "http://DEINE_VPS_IP:8484/?key=DEIN_KEY"

Beispielantwort:

    {"version":"…","uptime_s":3600,"groups":1,"group_list":[
      {"index":0,"srt_connected":true,"connections":[
        {"addr":"10.0.0.5","port":1234,"idle_s":0,"pkts":84213,"bytes":121000000},
        {"addr":"10.0.0.6","port":1235,"idle_s":1,"pkts":311,"bytes":410000}
      ]}]}

**Sicherheit:** Der Endpoint ist read-only und gibt die geheime Gruppen-ID
nicht aus. Wird er nach außen geöffnet (`STATS_ADDR=0.0.0.0`), **immer** einen
`STATS_KEY` setzen – sonst kann jeder die Modem-Adressen auslesen. Da der Key
über HTTP (unverschlüsselt) läuft, ist für „richtig öffentlich" ein
Reverse-Proxy mit HTTPS (Caddy/nginx) oder ein privates Netz (z. B. Tailscale)
zu empfehlen.

------------------------------------------------------------------------------
*Ab hier folgt die ursprüngliche README von BELABOX/srtla (Englisch, technische
Referenz – Protokollbeschreibung, Beispielaufrufe, Hintergrund).*

The server component - srtla_rec - in this repository is unsupported, no longer under development and not suitable for production deployment. Sign up for a [BELABOX cloud](https://belabox.net/cloud) account to benefit from the latest improvements, available on a global network of relay servers.
=====

srtla - SRT transport proxy with link aggregation for connection bonding
=====

*This is srtla2, incompatible with previous versions of srtla. Remember to update srtla both on the receiver and the sender*. srtla2 brings srtla_rec support for multiple simultaneous SRT streams, and many reliability improvements both for `srtla_send` and `srtla_rec`.

This tool can transport [SRT](https://github.com/Haivision/srt/) traffic over multiple network links for capacity aggregation and redundancy. Traffic is balanced dynamically, depending on the network conditions. The intended application is bonding mobile modems for live streaming.

This application is experimental. Be prepared to troubleshoot it and experiment with various settings for your needs.


Assumptions and prerequisites
-----------------------------

This tool assumes that data is streamed from a SRT *sender* in *caller* mode to a SRT *receiver* in *listener* mode. To get any benefit over using SRT directly, the *sender* should have 2 or more network links to the SRT listener (in the typical application, these would be internet-connected 4G modems). The sender needs to have [source routing](https://tldp.org/HOWTO/Adv-Routing-HOWTO/lartc.rpdb.simple.html) configured, as srtla uses `bind()` to map UDP sockets to a given connection. Only Linux is supported, however porting to other *nix platforms should be straightforward if needed. Update: `srtla_rec` has now been confirmed to work on Windows via WSL1.


Building (both on the sender and the receiver)
----------------------------------------------

    git clone https://github.com/BELABOX/srtla.git
    cd srtla
    make
    
This will produce 2 executables: `srtla_send` and `srtla_rec`.


Building the patched SRT (only needed on the receiver)
------------------------------------------------------

Our patched SRT library should be used on the `receiver`. srtla will work using the upstream SRT library, however performance may be degraded because the current upstream version causes excessive retransmissions when packets arrive out of order.

    git clone https://github.com/BELABOX/srt.git
    cd srt
    ./configure
    make

See the SRT documentation for more information on building it.


Example usage
-------------

Let's assume that the receiver has IP address 10.0.0.1 and the sender has 2 (unreliable) modems with IP addresses 192.168.0.2 and 192.168.1.2 respectively, which can reach the receiver. We'll set up the srtla receiver to run on port 5000 and we'll use `srt-live-transmit` (from our SRT repository) in listener mode to make the stream available on `0.0.0.0:5001` for local or remote clients that use the upstream SRT library.

**Receiver**

    path/to/srt/srt-live-transmit -st:yes "srt://127.0.0.1:5002?mode=listener&lossmaxttl=40&latency=2000" "srt://0.0.0.0:5001?mode=listener" &
    path/to/srtla/srtla_rec 5000 127.0.0.1 5002

    
Notes: `lossmaxttl` is a required option to allow packets to arrive out-of-order without immediately sending NAKs to ask for retransmission. Its value is the size of the receive window. Values between 10 and 50 are probably a reasonable starting point. The NAKs sent by SRT are used by srtla to balance the traffic between the links and lower `lossmaxttl` values will create a stronger bias towards using the faster networks disproportionately. If the window is too small, that may cause excessive retransmissions and it may prevent link aggregation from working by sending most of the traffic through a single link. If the window is too large, it may prevent timely retransmission of lost / late / corrupted packets and therefore data loss. `latency` (in ms) will determine the time available for retransmission and packet reordering (together with `lossmaxttl`).

**Sender**

    echo 192.168.0.2 > /tmp/srtla_ips
    echo 192.168.1.2 >> /tmp/srtla_ips
    path/to/srtla/srtla_send 6000 10.0.0.1 5000 /tmp/srtla_ips
    
With `srtla_send` running on the sender, SRT-enabled applications should stream to port `6000` on the sender and this data will be forwarded through srtla and srt-live-transmit to port `5001` on the receiver.

Note that instead of `srt-live-transmit`, you can directly use the end SRT application in listener mode on the receiver. It **must** be configured with the same options discussed above for srt-live-transmit and it **should** be linked against our modified SRT library.

Note that this basic setup doesn't implement authentication or encryption and `srt-live-transmit` can only handle one connection at a time.

Note that the sender **should** implement congestion control using adaptive bitrate based on the SRT `SRTO_SNDDATA` size or on the measured `RTT`. Also note that due to reordering, these values may be slightly higher during uncongested operation over srtla compared to direct SRT operation over one of the same network links.


How does it work?
-----------------

The core idea is that srtla keeps track of the number of packets in flight (sent but unacknowledged) for each link, together with a dynamic window size that tracks the capacity of each link - similarly to TCP congestion control. These are used together to balance the traffic through each link proportionally to its capacity. However, note that no congestion control is applied.

This assumes that you're familiar with the [SRT spec](https://tools.ietf.org/html/draft-sharabayko-mops-srt-00). TODO: technical description in more detail.

**srtla v2**

The main improvement in srtla v2 is that it supports multiple *srtla senders* connecting to a single *srtla receiver* by establishing *connection groups* - note that these are different from the experimental *socket groups* in SRT. To support this feature, I've introduced a 2-phase connection registration phase.

Normal registration:

* Sender (conn 0):   `SRTLA_REG1(sender_id = SRTLA_ID_LEN bytes sender-generated random id)`
* Receiver:          `SRTLA_REG2(full_id = sender_id with the last SRTLA_ID_LEN/2 bytes replaced with receiver-generated values)`
* Sender (conn 0):   `SRTLA_REG2(full_id)`
* Receiver:          `SRTLA_REG3`
* [...]
* Sender (conn n):   `SRTLA_REG2(full_id)`
* Receiver:          `SRTLA_REG3`


Error responses are only sent from the *receiver*. If the *sender* encounters an error, it should just abandon the relevant *connection group* or *connection*, and it will be garbage collected on the receiver side after some time. Possible error responses are sent after receiving a `SRTLA_REG1` or `SRTLA_REG2` request.

* `SRTLA_REG_ERR` - Can be sent in response to any `SRTLA_REG` command. Operation temporarily failed, back off and retry later.
* `SRTLA_REG_NAK` - Sent in response to `SRTLA_REG1`. Operation refused, give up. Either incompatible or access denied. A human-readable error message may be appended after the header.
* `SRTLA_REG_NGP` - Sent in response to `SRTLA_REG2` with an invalid ID. Register the group again with `SRTLA_REG1`
