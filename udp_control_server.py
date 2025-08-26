#!/usr/bin/env python3
# udp_control_server.py
"""
UDP-Steuerserver für Unitree-Roboter (Go2 SDK v2).
Start:  sudo python3 udp_control_server.py <netz-interface> [<listen_port>]

Nachrichtenformat (JSON, UTF-8, <= 256 Bytes):
{
  "seq"  : 123,                      # monoton steigende Sequenznummer
  "ts"   : 1720021234567890000,      # Unix-ns des Senders
  "token": "SECRET123",              # Pre-Shared Key (PSK)
  "cmd"  : "move",                  # Befehl (siehe CMD_MAP)
  "args" : {"vx":0.3,"vy":0,"w":0}   # optionale Parameter
}

Verlorene Pakete sind unkritisch; kommen > WATCHDOG_MS keine neuen,
stoppt der Roboter failsafe (StopMove).
"""
from __future__ import annotations
import asyncio, json, math, sys, time, hmac, hashlib, logging, threading, subprocess, os, base64
from datetime import datetime
from dataclasses import asdict, is_dataclass, dataclass
from numbers import Real
from collections.abc import Mapping, Sequence
from typing import Callable, Any, Dict

from unitree_sdk2py.core.channel import ChannelFactoryInitialize
from unitree_sdk2py.go2.sport.sport_client import SportClient
from unitree_sdk2py.go2.obstacles_avoid.obstacles_avoid_client import ObstaclesAvoidClient
from unitree_sdk2py.idl.unitree_go.msg.dds_ import LowState_
from unitree_sdk2py.core.channel import ChannelSubscriber
from unitree_sdk2py.go2.video.video_client import VideoClient

# --------------------------------------------------------------------------- #
#                         ► Konfiguration & Konstanten                        #
# --------------------------------------------------------------------------- #

LISTEN_IP      = "0.0.0.0"
LISTEN_PORT    = 5555            # kann per CLI überschrieben werden
MAX_DGRAM      = 512             # Byte-Budget
TOKEN          = b"CHANGE_ME"    # PSK – mind. 16 random Bytes!
CLOCK_DRIFT_S  = 2.0             # ältere Pakete ⇒ Drop (Replay-Schutz)
WATCHDOG_MS    = 300             # Roboter stoppt nach 300 ms Funkstille
LOG_LEVEL      = logging.INFO
# Vorsicht: Base64 vergrößert um ~33%. Wir halten Chunk klein, damit Header + JSON passen.
IMAGE_B64_CHUNK = 300

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d  %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("udp_control")


# ----------------------- UDP‑Log‑Handler (Server → Clients) ---------------
class UDPLogHandler(logging.Handler):
    """Schickt jeden Log‑Record als JSON an alle abonnierten Peers."""
    def __init__(self, proto: "UDPCommandProtocol"):
        super().__init__()
        self.proto = proto
        self.setFormatter(logging.Formatter(
            "%(asctime)s.%(msecs)03d  %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        ))

    def emit(self, record: logging.LogRecord):
        if not self.proto.transport:
            return
        msg_txt = self.format(record)
        pkt = json.dumps({
            "log":   msg_txt,
            "level": record.levelname,
            "ts":    int(record.created * 1e9),  # ns
        }).encode()
        for peer in list(self.proto.log_subs):
            try:
                self.proto.transport.sendto(pkt, peer)
            except (OSError, AttributeError):
                self.proto.log_subs.discard(peer)

# --------------------------------------------------------------------------- #
#                             Low-State Subscription                           #
# --------------------------------------------------------------------------- #
class LowStateMonitor:
    
    ROUND_NDIGITS = 3 
    """Thread-safe Aufbewahrung des letzten LowState-Samples."""
    def __init__(self) -> None:
        self._latest: LowState_ | None = None
        self._lock   = threading.Lock()

        # DDS-Subscriber starten (separater Thread in der RT-Mittelware)
        self._sub = ChannelSubscriber("rt/lowstate", LowState_)
        # QoS depth = 1 reicht: wir wollen nur das jüngste Sample
        self._sub.Init(self._handler, 1)

    # DDS-Callback (läuft NICHT im asyncio-Thread!)
    def _handler(self, msg: LowState_):
        with self._lock:
            self._latest = msg

    def _round(self, val):
            return round(val, self.ROUND_NDIGITS) if isinstance(val, Real) and not isinstance(val, bool) else val
        
    def _jsonable(self, val):
        """bytes → Liste[int]; alles andere unverändert zurückgeben."""
        if isinstance(val, (bytes, bytearray)):
            return list(val)
        return val

    def _flatten(self, obj, prefix=""):
        flat = {}

        # Dataclass → dict
        if is_dataclass(obj):
            obj = asdict(obj)

        # Mapping
        if isinstance(obj, Mapping):
            for k, v in obj.items():
                flat.update(self._flatten(v, f"{prefix}{k}_"))

        # Sequenz (aber kein str/bytes)
        elif isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
            for idx, v in enumerate(obj):
                flat.update(self._flatten(v, f"{prefix}{idx}_"))

        # Skalar
        else:
            flat[prefix[:-1]] = self._round(self._jsonable(obj))

        return flat

    # ------------------------------- API ----------------------------------- #
    def snapshot(self, include: set[str] | None = None) -> dict | None:
        """
        Liefert ein flaches JSON-Dict.
        Falls *include* gesetzt ist, bleiben nur Keys erhalten, die
        (1) exakt in include stehen  oder
        (2) mit einem der Einträge beginnen.
        """
        with self._lock:
            if not self._latest:
                return None

            flat = self._flatten(self._latest)

            if not include:                 # kein Filter ⇒ alles schicken
                return flat

            def keep(k: str) -> bool:
                return any(k == p or k.startswith(p) for p in include)

            return {k: v for k, v in flat.items() if keep(k)}

# --------------------------------------------------------------------------- #
#                               Sport-Befehle                                 #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Cmd:
    fn:      Callable[[SportClient, dict], Any]
    need_arg: bool = False        # ob 'args' zwingend erforderlich


def _noop(cli: SportClient, _):             # nur als Platzhalter
    pass

CMD_MAP: Dict[str, Cmd] = {
    "hb"            : Cmd(lambda s, _: None),   # Health-Check, keine Aktion
    "damp"          : Cmd(lambda s, _: s.Damp()),
    "stand_up"      : Cmd(lambda s, _: s.StandUp()),
    "stand_down"    : Cmd(lambda s, _: s.StandDown()),
    "stop"          : Cmd(lambda s, _: s.StopMove()),
    "switch_gait0"  : Cmd(lambda s, _: s.SwitchGait(0)),
    "switch_gait1"  : Cmd(lambda s, _: s.SwitchGait(1)),
    "balance"       : Cmd(lambda s, _: s.BalanceStand()),
    "recover"       : Cmd(lambda s, _: s.RecoveryStand()),
    "left_flip"     : Cmd(lambda s, _: s.LeftFlip()),
    "back_flip"     : Cmd(lambda s, _: s.BackFlip()),
    "move"          : Cmd(
        lambda s, a: s.Move(
            float(a.get("vx", 0)),
            float(a.get("vy", 0)),
            float(a.get("w" , 0)),
        ),
        need_arg=True,
    ),
    # --- System‑Befehle --------------------------------------------------
    # Reboot des gesamten Hosts.  **Vorsicht:** stoppt laufende Prozesse!
    "reboot"        : Cmd(
        lambda _s, _a: subprocess.Popen(
            ["systemctl", "reboot", "--no-wall"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    ),
    # --- Logging‑Subscription ----------------------------------------
    "log_sub"       : Cmd(lambda _s, _a: None),   # handled inline
    "log_unsub"     : Cmd(lambda _s, _a: None),   # handled inline
    # weitere Befehle lassen sich hier einzeilig ergänzen …
}

# --------------------------------------------------------------------------- #
#                           Datagram-Protokoll-Handler                        #
# --------------------------------------------------------------------------- #

class UDPCommandProtocol(asyncio.DatagramProtocol):
    def __init__(self, sport: SportClient, oa: ObstaclesAvoidClient, lowstate: LowStateMonitor, video: VideoClient) -> None:
        self.sport            = sport
        self.obstacles_avoid  = oa
        self.last_seq_by_peer: Dict[tuple[str, int], int] = {}
        self.last_ts_by_peer:  Dict[tuple[str, int], int] = {}
        self.last_cmd_ts: float = time.monotonic()
        self.transport: asyncio.DatagramTransport | None = None
        self.lowstate = lowstate
        self.log_subs: set[tuple[str, int]] = set()
        self.video = video
    def connection_made(self, transport):
        self.transport = transport

    # ~~~~~~~~~~~~~~~~~~~~~~~~~ Datagram-Callback ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
    def datagram_received(self, data: bytes, addr):
        now = time.time()
        if len(data) > MAX_DGRAM:
            log.warning("Dropping oversize packet from %s", addr)
            return

        try:
            msg = json.loads(data.decode("utf-8"))
            log.debug("RX %s -> %s", addr, msg)
        except Exception:
            log.warning("Bad JSON from %s", addr)
            return

        # --------- Schnelle Auth + Replay-Schutz --------------------------- #
        if not self._auth_ok(msg):
            log.warning("Auth failed from %s", addr)
            return

        try:
            ts_ns = int(msg.get("ts", 0))
        except (TypeError, ValueError):
            log.warning("Bad ts from %s", addr)
            return

        if abs(now - ts_ns / 1e9) > CLOCK_DRIFT_S:
            log.warning("Stale packet from %s (ts=%s)", addr, ts_ns)
            return 
        
        # --------- Replay- und Neustart-Erkennung -------------------------- #
        peer = addr                                   # (ip, port)

        last_seq = self.last_seq_by_peer.get(peer, -1)
        last_ts  = self.last_ts_by_peer .get(peer, 0)

        # Nur wenn *beides* nicht frischer ist → echtes Replay ⇒ Drop
        if msg["seq"] <= last_seq and msg["ts"] <= last_ts:
            log.debug("Replay/out-of-order from %s (seq %s ≤ %s, ts %s ≤ %s)",
                      peer, msg["seq"], last_seq, msg["ts"], last_ts)
            return

        # Update Tracking – akzeptiere Paket (auch nach Client-Neustart)
        self.last_seq_by_peer[peer] = msg["seq"]
        self.last_ts_by_peer [peer] = msg["ts"]


        # --------- Befehl ausführen --------------------------------------- #
        cmd_name = msg.get("cmd")
        args = msg.get("args") or {}
        print(args)
         # --------- MOVE → ObstaclesAvoid -------------------------------- #
        if cmd_name == "log_sub":               # <— LOG‑SUBSCRIBE
            self.log_subs.add(peer)
            log.info("Added log subscriber %s", peer)
            return
        elif cmd_name == "log_unsub":           # <— LOG‑UNSUBSCRIBE
            self.log_subs.discard(peer)
            log.info("Removed log subscriber %s", peer)
            return
        elif cmd_name == "oa.move":
            vx = float(args.get("vx", 0))
            vy = float(args.get("vy", 0))
            w  = float(args.get("w" , 0))
            try:
                self.obstacles_avoid.Move(vx, vy, w)
                log.info("OA.Move  vx=%g vy=%g w=%g", vx, vy, w)
            except Exception as e:
                log.exception("OA.Move error: %s", e)
        elif cmd_name == "get_lowstate":
            filter_active = True
            # ------------- Filterliste aus args ---------------------------------- #
            filt_raw = ["bms_state_soc", "bms_state_current", "imu_state_quaternion_", "imu_state_gyroscope_", "imu_state_accelerometer", "imu_state_rpy", "foot_force_" ]
            if filt_raw and not isinstance(filt_raw, (list, tuple, set)):
                log.warning("filter must be list/tuple/set – ignored")
                filt_raw = None

            include = set(map(str, filt_raw)) if filt_raw else None
            if filter_active:
                ls = self.lowstate.snapshot(include)   # <-- Filter anwenden
            else:
                ls = self.lowstate.snapshot()
            resp = {"err": "no-lowstate"} if ls is None else {"lowstate": ls}

            # kompakt zurücksenden
            self.transport.sendto(
                json.dumps(resp, separators=(",", ":"), ensure_ascii=False).encode(),
                addr
            )
            return
        elif cmd_name == "get_image":
            try:
                ret, bin_data = self.video.GetImageSample()
                if ret != 0 or not bin_data:
                    self.transport.sendto(
                        json.dumps({"err": "no-image", "code": ret}, separators=(",", ":"), ensure_ascii=False).encode(),
                        addr
                    )
                    return
                # bin_data kann je nach Binding bytes/bytearray/list[int]/tuple[int] sein → in bytes umwandeln
                if isinstance(bin_data, (bytes, bytearray)):
                    raw = bytes(bin_data)
                elif isinstance(bin_data, (list, tuple)):
                    try:
                        raw = bytes(bin_data)
                    except Exception:
                        self.transport.sendto(
                            json.dumps({"err": "bad-image-type"}, separators=(",", ":"), ensure_ascii=False).encode(),
                            addr
                        )
                        return
                else:
                    try:
                        raw = memoryview(bin_data).tobytes()
                    except Exception:
                        self.transport.sendto(
                            json.dumps({"err": "bad-image-type"}, separators=(",", ":"), ensure_ascii=False).encode(),
                            addr
                        )
                        return
                # base64 encodieren und in UDP-taugliche JSON-Chunks zerlegen
                b64 = base64.b64encode(raw).decode("ascii")
                total_chunks = max(1, math.ceil(len(b64) / IMAGE_B64_CHUNK))
                img_id = int(time.time() * 1e6)  # einfache ID
                # Header schicken
                header = {"img": {"id": img_id, "n": total_chunks, "size": len(raw)}}
                self.transport.sendto(
                    json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode(),
                    addr
                )
                # Daten-Chunks schicken
                for idx in range(total_chunks):
                    piece = b64[idx * IMAGE_B64_CHUNK:(idx + 1) * IMAGE_B64_CHUNK]
                    pkt = {"img": {"id": img_id, "i": idx, "n": total_chunks, "data": piece}}
                    self.transport.sendto(
                        json.dumps(pkt, separators=(",", ":"), ensure_ascii=False).encode(),
                        addr
                    )
                log.info("Sent image id=%s in %d chunk(s), %d bytes raw", img_id, total_chunks, len(raw))
            except Exception as e:
                log.exception("GetImageSample error: %s", e)
            return
        else:
            cmd = CMD_MAP.get(cmd_name)
            if not cmd:
                log.warning("Unknown cmd '%s' from %s", cmd_name, addr)
                return
            if cmd.need_arg and not args:
                log.warning("Cmd '%s' requires args – ignored", cmd_name)
                return
            try:
                cmd.fn(self.sport, args)
                log.info("Executed %s args=%s", cmd_name, args)
            except Exception as e:
                log.exception("SportClient error on %s: %s", cmd_name, e)


        self.last_cmd_ts = time.monotonic()
        
           # ------------ Echo zurück  (loss & rtt optional) -------------- #
        try:
            # primitive Loss-Schätzung: 1 wenn seq-Sprung >1
            last_seen = self.last_seq_by_peer.get(peer, None)
            lost = max(0, msg["seq"] - 1 - last_seen) if last_seen is not None else 0
            loss_pct = 100 if msg["seq"]==0 else int(100 * lost / max(1, msg["seq"]))

            rtt_ms = max(0, int((time.time() - ts_ns / 1e9) * 1000))

            echo = {"ack": msg.get("seq"), "loss": loss_pct, "rtt": rtt_ms}
            #self.transport.sendto(json.dumps(echo).encode(), addr)
        except Exception:
            pass

    # ------------------------------- Helpers ------------------------------- #
    def _auth_ok(self, msg: dict) -> bool:
        token_field = msg.get("token")
        if not token_field:
            return False
        # hier reicht String-Vergleich; für höhere Sicherheit HMAC benutzen:
        return hmac.compare_digest(token_field.encode(), TOKEN)

    async def watchdog(self):
        """Stoppt den Roboter, wenn länger als WATCHDOG_MS kein Kommando kam."""
        while True:
            await asyncio.sleep(WATCHDOG_MS / 1000 / 2)
            if time.monotonic() - self.last_cmd_ts > WATCHDOG_MS / 1000:
                try:
                    self.sport.StopMove()
                    log.warning("Watchdog: auto-StopMove() triggered")
                except Exception:
                    log.exception("Watchdog StopMove failed")
                self.last_cmd_ts = time.monotonic()  # verhindert Dauerschleife


# --------------------------------------------------------------------------- #
#                                    Main                                     #
# --------------------------------------------------------------------------- #

async def main():
    if len(sys.argv) < 2:
        print(f"Usage: sudo {sys.argv[0]} <networkInterface> [<port>]")
        sys.exit(1)

    iface = sys.argv[1]
    port  = int(sys.argv[2]) if len(sys.argv) > 2 else LISTEN_PORT

    # ---------- Unitree DDS transport initialisieren ---------------------- #
    ChannelFactoryInitialize(0, iface)

    sport = SportClient()
    sport.SetTimeout(10.0)
    sport.Init()
    log.info("SportClient initialised")
    oa = ObstaclesAvoidClient()
    oa.SetTimeout(20.0)
    oa.Init()
    oa.SwitchSet(True)
    oa.UseRemoteCommandFromApi(True)
    log.info("ObstaclesAvoidClient initialised")
    lowstate = LowStateMonitor()
    log.info("LowStateMonitor initialised")
    video = VideoClient()
    video.SetTimeout(20.0)
    video.Init()
    log.info("VideoClient initialised")
    loop = asyncio.get_running_loop()
    transport, proto = await loop.create_datagram_endpoint(
        lambda: UDPCommandProtocol(sport, oa, lowstate, video),
        local_addr=(LISTEN_IP, port),
        reuse_port=True,
    )       
    log.info("Listening on UDP %s:%d", LISTEN_IP, port)
    log.addHandler(UDPLogHandler(proto))
    # separater Watchdog-Task
    asyncio.create_task(proto.watchdog())

    try:
        await asyncio.Future()          # run forever
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCtrl-C – bye.")