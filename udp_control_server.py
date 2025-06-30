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
import asyncio, json, math, sys, time, hmac, hashlib, logging
from dataclasses import dataclass
from typing import Callable, Any, Dict

from unitree_sdk2py.core.channel import ChannelFactoryInitialize
from unitree_sdk2py.go2.sport.sport_client import SportClient
from unitree_sdk2py.go2.obstacles_avoid.obstacles_avoid_client import ObstaclesAvoidClient
from unitree_sdk2py.go2.robot_state.robot_state_client import RobotStateClient


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

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d  %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("udp_control")

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
    # weitere Befehle lassen sich hier einzeilig ergänzen …
}

# --------------------------------------------------------------------------- #
#                           Datagram-Protokoll-Handler                        #
# --------------------------------------------------------------------------- #

class UDPCommandProtocol(asyncio.DatagramProtocol):
    def __init__(self, sport: SportClient, oa: ObstaclesAvoidClient) -> None:
        self.sport            = sport
        self.obstacles_avoid  = oa
        self.last_seq_by_peer: Dict[tuple[str, int], int] = {}
        self.last_ts_by_peer:  Dict[tuple[str, int], int] = {}
        self.last_cmd_ts: float = time.monotonic()
        self.transport: asyncio.DatagramTransport | None = None

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
        if cmd_name == "oa.move":
            vx = float(args.get("vx", 0))
            vy = float(args.get("vy", 0))
            w  = float(args.get("w" , 0))
            try:
                self.obstacles_avoid.Move(vx, vy, w)
                log.info("OA.Move  vx=%g vy=%g w=%g", vx, vy, w)
            except Exception as e:
                log.exception("OA.Move error: %s", e)
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
            self.transport.sendto(json.dumps(echo).encode(), addr)
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
    oa.Init()
    oa.SwitchSet(True)
    oa.UseRemoteCommandFromApi(True)
    log.info("ObstaclesAvoidClient initialised")
    # state = RobotStateClient()
    # state.Init()
    # state.SetReportFreq(1000, 10000)
    # code, services = state.ServiceList()
    # if code == 0:
    #     for svc in services:
    #         log.info("Srv %-18s status=%d protect=%s",
    #                 svc.name, svc.status, svc.protect)
    # else:
    #     log.error("ServiceList failed (%d)", code)
    # log.info("RobotStateClient initialised")

    loop = asyncio.get_running_loop()
    transport, proto = await loop.create_datagram_endpoint(
        lambda: UDPCommandProtocol(sport, oa),
        local_addr=(LISTEN_IP, port),
        reuse_port=True,
    )
    log.info("Listening on UDP %s:%d", LISTEN_IP, port)

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
