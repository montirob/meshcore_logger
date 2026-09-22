#!/usr/bin/env python3
"""Logger MeshCore — legge la telemetria ambientale (LPP Cayenne) e la salva
nella STESSA tabella `readings` usata dalla dashboard (così grafici/API restano
identici). Basato sull'API reale della libreria `meshcore` (asyncio).

BOZZA da validare alla prima connessione reale: la forma esatta del payload
`TELEMETRY_RESPONSE` e il flusso connect vanno confermati col nodo collegato.

Connessione (env MC_CONN): serial | tcp | ble
  serial: MC_PORT=/dev/ttyACM0  MC_BAUD=115200
  tcp:    MC_HOST=<ip|hostname|auto>  MC_TCP_PORT=5000
          (se MC_HOST è vuoto/'auto' o non risponde, il nodo viene cercato
           automaticamente sulla LAN scandendo la porta companion → robusto ai
           cambi di IP DHCP)
  ble:    MC_BLE_ADDR=<mac>  MC_BLE_PIN=<pin opz>
Sensore (env MC_SENSOR): 'self' = nodo collegato al Pi;
  altrimenti nome (o prefisso pubkey) di un contatto remoto sulla mesh.
"""
import os, time, json, sqlite3, asyncio, inspect, datetime, socket
import concurrent.futures as _cf
from meshcore import MeshCore, EventType

DB       = os.environ.get("MESH_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "meshlogger.db"))
CONN     = os.environ.get("MC_CONN", "serial").lower()
PORT     = os.environ.get("MC_PORT", "/dev/ttyACM0")
BAUD     = int(os.environ.get("MC_BAUD", "115200"))
HOST     = os.environ.get("MC_HOST", "")
TCP_PORT = int(os.environ.get("MC_TCP_PORT", "5000"))
BLE_ADDR = os.environ.get("MC_BLE_ADDR", "") or None
BLE_PIN  = os.environ.get("MC_BLE_PIN", "") or None
SENSOR   = os.environ.get("MC_SENSOR", "self")
INTERVAL = int(os.environ.get("MESH_INTERVAL", "60"))
NUM_CHANNELS = int(os.environ.get("MC_NUM_CHANNELS", "8"))
NODES_REFRESH = int(os.environ.get("MC_NODES_REFRESH", "300"))
DM_CHANNEL = -1   # canale convenzionale dei messaggi diretti nella tabella `messages`
# Range fisici del sensore: fuori da questi valori la lettura è un guasto (es. picchi
# elettrici) e va scartata, non salvata.
LIMITS = {"temperature": (-40.0, 85.0), "humidity": (0.0, 100.0), "pressure": (300.0, 1100.0)}
# --- Robustezza / anti-blocco (connessione TCP che si "appende" senza chiudersi) ---
CONNECT_TIMEOUT = int(os.environ.get("MC_CONNECT_TIMEOUT", "30"))                          # s max per connettersi
CALL_TIMEOUT    = int(os.environ.get("MC_CALL_TIMEOUT", "25"))                             # s max per una richiesta al nodo
STALE_RECONNECT = int(os.environ.get("MC_STALE_RECONNECT", str(max(240, INTERVAL * 4))))  # s senza dati salvati -> riconnessione forzata
STALL_LIMIT     = int(os.environ.get("MC_STALL_LIMIT", str(max(300, INTERVAL * 5))))       # s senza progressi -> restart processo (watchdog)

def log(*a):
    print(datetime.datetime.now().isoformat(timespec="seconds"), *a, flush=True)

# --- Scoperta automatica del nodo sulla LAN (robustezza ai cambi di IP) ---
def _local_ip():
    """IP locale primario (senza inviare traffico): usato per dedurre la /24."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def _port_open(host, port, timeout=0.5):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False

def discover_tcp_host(port, exclude=None):
    """Scansiona la /24 locale cercando un nodo con la porta companion aperta.
    Ritorna il primo IP trovato (numericamente più basso) o None."""
    myip = _local_ip()
    prefix = myip.rsplit(".", 1)[0]
    skip = set(exclude or []); skip.add(myip)
    hosts = [f"{prefix}.{i}" for i in range(1, 255)]
    found = []
    with _cf.ThreadPoolExecutor(max_workers=64) as ex:
        for h, ok in zip(hosts, ex.map(lambda h: _port_open(h, port, 0.4), hosts)):
            if ok and h not in skip:
                found.append(h)
    found.sort(key=lambda h: int(h.rsplit(".", 1)[1]))
    return found[0] if found else None

def resolve_tcp_host():
    """Determina l'host del nodo: usa MC_HOST se raggiungibile, altrimenti lo cerca
    sulla LAN scandendo la porta companion. Sincrona (chiamata via executor)."""
    if HOST and HOST.lower() != "auto":
        if _port_open(HOST, TCP_PORT, 1.0):
            return HOST
        log(f"MC_HOST={HOST} non risponde su :{TCP_PORT} → ricerca del nodo sulla LAN…")
    else:
        log("MC_HOST=auto → ricerca del nodo sulla LAN…")
    h = discover_tcp_host(TCP_PORT, exclude=[HOST] if HOST else None)
    if h:
        log(f"nodo MeshCore trovato: {h}:{TCP_PORT}")
        return h
    log("nessun nodo trovato sulla LAN; riprovo con MC_HOST invariato")
    return HOST

async def _maybe(x):
    """Attende x se è una coroutine, altrimenti lo ritorna (API sync/async-agnostica)."""
    return await x if inspect.iscoroutine(x) else x

# --- Watchdog anti-blocco -------------------------------------------------
# Se il loop principale non fa progressi per STALL_LIMIT secondi (tipico: await
# appeso su un socket TCP half-open dopo un riavvio/drop del nodo), il processo
# esce: systemd (Restart=always) lo riavvia pulito. NB: mentre il nodo è spento
# il loop RITENTA attivamente -> è un progresso, quindi il watchdog NON scatta.
_LAST_PROGRESS = time.time()
def _progress():
    global _LAST_PROGRESS
    _LAST_PROGRESS = time.time()

async def _watchdog():
    while True:
        await asyncio.sleep(15)
        stalled = time.time() - _LAST_PROGRESS
        if stalled > STALL_LIMIT:
            log(f"WATCHDOG: nessun progresso da {int(stalled)}s (> {STALL_LIMIT}s) -> riavvio processo")
            os._exit(1)

def init_db():
    con = sqlite3.connect(DB)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=8000")
    except Exception:
        pass
    con.execute("""CREATE TABLE IF NOT EXISTS readings(
        ts INTEGER PRIMARY KEY, iso TEXT,
        temperature REAL, humidity REAL, pressure REAL, metrics TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS channels(
        idx INTEGER PRIMARY KEY, name TEXT, role INTEGER)""")
    con.execute("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER, iso TEXT, from_id TEXT, from_name TEXT,
        to_id TEXT, channel INTEGER, msg_id INTEGER, text TEXT,
        outgoing INTEGER DEFAULT 0, reply_id INTEGER,
        UNIQUE(from_id, msg_id, text))""")
    for ddl in ("ALTER TABLE messages ADD COLUMN path TEXT",
                "ALTER TABLE messages ADD COLUMN path_len INTEGER",
                "ALTER TABLE messages ADD COLUMN snr REAL"):
        try:
            con.execute(ddl)
        except sqlite3.OperationalError:
            pass
    con.execute("CREATE INDEX IF NOT EXISTS idx_msg_ts ON messages(ts)")
    con.execute("""CREATE TABLE IF NOT EXISTS outbox(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER,
        channel INTEGER, text TEXT, status TEXT DEFAULT 'pending', error TEXT, reply_id INTEGER)""")
    try:
        con.execute("ALTER TABLE outbox ADD COLUMN to_id TEXT")   # destinatario dei messaggi diretti
    except sqlite3.OperationalError:
        pass
    con.execute("""CREATE TABLE IF NOT EXISTS nodes(
        node_id TEXT PRIMARY KEY, num INTEGER, long_name TEXT, short_name TEXT,
        hw TEXT, role TEXT, last_heard INTEGER, snr REAL, hops INTEGER,
        battery INTEGER, voltage REAL, has_env INTEGER, lat REAL, lon REAL, updated INTEGER,
        tracked INTEGER DEFAULT 0)""")
    con.execute("""CREATE TABLE IF NOT EXISTS positions(
        id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT, ts INTEGER, lat REAL, lon REAL, alt REAL)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_pos ON positions(node_id, ts)")
    con.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("""CREATE TABLE IF NOT EXISTS mc_commands(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, action TEXT, params TEXT,
        status TEXT DEFAULT 'pending', result TEXT, error TEXT, done_ts INTEGER)""")
    # pulizia una-tantum dei nodi/posizioni dell'era Meshtastic (id tipo '!hex')
    con.execute("DELETE FROM nodes WHERE node_id LIKE '!%'")
    con.execute("DELETE FROM positions WHERE node_id LIKE '!%'")
    con.commit()
    return con

def _lpp_items(payload):
    try:
        from meshcore.lpp_json_encoder import lpp_json_encoder, LppFrame
        if isinstance(payload, LppFrame):
            return json.loads(json.dumps(payload, default=lpp_json_encoder))
        if isinstance(payload, (list, tuple)):
            return json.loads(json.dumps(list(payload), default=lpp_json_encoder))
        if isinstance(payload, dict):
            return payload.get("lpp") or payload.get("telemetry") or []
        return json.loads(json.dumps(payload, default=lpp_json_encoder))
    except Exception as e:
        log("lpp parse warn:", e, "| payload:", repr(payload)[:200])
        return []

def parse_lpp(payload):
    """Estrae {temperature,humidity,pressure} dal canale AMBIENTALE (il BME:
    quello che contiene umidità o barometro). Ignora la temperatura di scheda
    (canale con la sola tensione)."""
    items = _lpp_items(payload)
    if isinstance(items, dict):
        items = [items]
    bychan = {}
    for it in (items or []):
        if not isinstance(it, dict):
            continue
        ch = it.get("channel")
        typ = str(it.get("type", "")).lower()
        val = it.get("value")
        if isinstance(val, dict):
            val = val.get("value", val)
        d = bychan.setdefault(ch, {})
        if "temp" in typ:
            d["temperature"] = val
        elif "humid" in typ:
            d["humidity"] = val
        elif "barom" in typ or "press" in typ:
            d["pressure"] = val
        elif "volt" in typ:
            d["voltage"] = val
        elif "alt" in typ:
            d["altitude"] = val
    # canale ambientale = quello che ha umidità o pressione (il BME)
    for ch, d in bychan.items():
        if "humidity" in d or "pressure" in d:
            return {k: d[k] for k in ("temperature", "humidity", "pressure") if k in d}
    return {}  # nessun dato ambientale in questa risposta

def plausible(m):
    """False se un valore è fuori dal range fisico del sensore (lettura corrotta)."""
    for k, (lo, hi) in LIMITS.items():
        v = m.get(k)
        if v is not None and not (lo <= v <= hi):
            return False
    return True

def save(con, m):
    now = int(time.time()); iso = datetime.datetime.now().isoformat(timespec="seconds")
    con.execute("INSERT OR REPLACE INTO readings(ts,iso,temperature,humidity,pressure,metrics) VALUES(?,?,?,?,?,?)",
                (now, iso, m.get("temperature"), m.get("humidity"), m.get("pressure"), json.dumps(m)))
    con.commit()

def _iso(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat(timespec="seconds")

def _split_name(text):
    """I messaggi di canale MeshCore sono 'Nome: testo'. Separa nome e testo."""
    if isinstance(text, str):
        i = text.find(": ")
        if 0 < i <= 32 and "\n" not in text[:i]:
            return text[:i], text[i + 2:]
    return None, text

_recent_logs = []  # RX log recenti dei messaggi di canale (GRP_TXT) per correlare il percorso

def on_rx_log(ev):
    try:
        pl = getattr(ev, "payload", None) or {}
        if not isinstance(pl, dict) or pl.get("payload_typename") != "GRP_TXT":
            return
        _recent_logs.append({"t": time.time(), "path": pl.get("path"),
                             "snr": pl.get("snr"), "path_len": pl.get("path_len"), "used": False})
        cutoff = time.time() - 30
        while _recent_logs and _recent_logs[0]["t"] < cutoff:
            _recent_logs.pop(0)
    except Exception:
        pass

def _match_recent_log():
    now = time.time()
    for r in reversed(_recent_logs):
        if r["used"]:
            continue
        if now - r["t"] > 12:
            break
        r["used"] = True
        return r
    return None

def on_chan_msg(ev):
    """Callback per i messaggi di canale ricevuti → tabella messages."""
    try:
        pl = getattr(ev, "payload", None) or {}
        if not isinstance(pl, dict):
            return
        ch = pl.get("channel_idx")
        text = pl.get("text", "") or ""
        ts = int(pl.get("sender_timestamp") or time.time())
        name, msg = _split_name(text)
        path = pl.get("path"); path_len = pl.get("path_len"); snr = pl.get("SNR")
        if not path:  # fallback: correla con l'RX log GRP_TXT più recente
            lr = _match_recent_log()
            if lr:
                path = lr.get("path")
                if snr is None: snr = lr.get("snr")
                if not path_len: path_len = lr.get("path_len")
        con = sqlite3.connect(DB)
        try: con.execute("PRAGMA busy_timeout=8000")
        except Exception: pass
        con.execute("""INSERT OR IGNORE INTO messages(ts,iso,from_id,from_name,to_id,channel,msg_id,text,outgoing,path,path_len,snr)
                       VALUES(?,?,?,?,?,?,?,?,0,?,?,?)""",
                    (ts, _iso(ts), None, name, None, ch, ts, msg, path, path_len, snr))
        con.commit(); con.close()
        log(f"MSG ch{ch} {name or '?'}: {msg} [salti={path_len} percorso={'sì' if path else 'no'} snr={snr}]")
    except Exception as e:
        log("on_chan_msg err:", e)

def on_contact_msg(ev):
    """Callback per i messaggi diretti (DM) ricevuti → tabella messages, canale -1."""
    try:
        pl = getattr(ev, "payload", None) or {}
        if not isinstance(pl, dict):
            return
        nid = (pl.get("pubkey_prefix") or "")[:12]
        text = pl.get("text", "") or ""
        ts = int(pl.get("sender_timestamp") or time.time())
        plen = pl.get("path_len")
        if plen == 255:   # 255 = consegna diretta, senza salti
            plen = 0
        con = sqlite3.connect(DB)
        try: con.execute("PRAGMA busy_timeout=8000")
        except Exception: pass
        row = con.execute("SELECT long_name FROM nodes WHERE node_id=?", (nid,)).fetchone()
        name = (row[0] if row and row[0] else nid)
        con.execute("""INSERT OR IGNORE INTO messages(ts,iso,from_id,from_name,to_id,channel,msg_id,text,outgoing,path_len,snr)
                       VALUES(?,?,?,?,NULL,?,?,?,0,?,?)""",
                    (ts, _iso(ts), nid, name, DM_CHANNEL, ts, text, plen, pl.get("SNR")))
        con.commit(); con.close()
        log(f"DM da {name}: {text} [salti={plen} snr={pl.get('SNR')}]")
    except Exception as e:
        log("on_contact_msg err:", e)

async def process_outbox(mc, con, myname):
    """Invia i messaggi in coda: sul canale (send_chan_msg) o diretti a un nodo
    (send_msg, quando la riga ha `to_id`), e li registra in `messages`."""
    try:
        pend = con.execute("SELECT id,channel,text,to_id FROM outbox WHERE status='pending' ORDER BY id").fetchall()
    except Exception:
        return
    for oid, ch, text, to_id in pend:
        try:
            if to_id:
                contact = mc.get_contact_by_key_prefix(to_id)
                if not contact:
                    raise RuntimeError("contatto non trovato: " + str(to_id))
                await asyncio.wait_for(_maybe(mc.commands.send_msg(contact, text)), CALL_TIMEOUT)
                ts = int(time.time())
                con.execute("UPDATE outbox SET status='sent' WHERE id=?", (oid,))
                con.execute("""INSERT INTO messages(ts,iso,from_id,from_name,to_id,channel,msg_id,text,outgoing)
                               VALUES(?,?,?,?,?,?,?,?,1)""",
                            (ts, _iso(ts), None, myname, to_id, DM_CHANNEL, None, text))
                con.commit()
                log(f"DM a {contact.get('adv_name') or to_id}: {text}")
                continue
            await _maybe(mc.commands.send_chan_msg(int(ch or 0), text))
            ts = int(time.time())
            con.execute("UPDATE outbox SET status='sent' WHERE id=?", (oid,))
            con.execute("""INSERT INTO messages(ts,iso,from_id,from_name,to_id,channel,msg_id,text,outgoing)
                           VALUES(?,?,?,?,?,?,?,?,1)""",
                        (ts, _iso(ts), None, myname, None, int(ch or 0), None, text))
            con.commit()
            log(f"SENT ch{ch}: {text}")
        except Exception as e:
            con.execute("UPDATE outbox SET status='error', error=? WHERE id=?", (str(e), oid)); con.commit()
            log("send err:", e)

TYPE_NAMES = {1: "Chat", 2: "Repeater", 3: "Room", 4: "Sensor", 5: "Sensor"}

async def dump_nodes_mc(mc, con):
    """Scarica i contatti MeshCore nella tabella `nodes` (upsert, preserva `tracked`)
    e registra le posizioni dei nodi tracciati quando cambiano."""
    try:
        try:
            await _maybe(mc.commands.get_contacts())
        except Exception:
            await _maybe(mc.ensure_contacts())
        cs = getattr(mc, "contacts", None) or {}
        items = cs.items() if isinstance(cs, dict) else enumerate(cs)
        now = int(time.time()); rows = []
        for k, v in items:
            if not isinstance(v, dict):
                continue
            pk = v.get("public_key") or ""
            nid = pk[:12] if pk else str(k)[:12]
            lat = v.get("adv_lat"); lon = v.get("adv_lon")
            if (lat in (0, 0.0)) and (lon in (0, 0.0)):
                lat = lon = None
            opl = v.get("out_path_len")
            hops = opl if isinstance(opl, int) and opl >= 0 else None
            t = v.get("type")
            role = TYPE_NAMES.get(t, ("tipo " + str(t)) if t is not None else None)
            rows.append((nid, None, v.get("adv_name"), None, None, role,
                         v.get("last_advert"), None, hops, None, None, 0, lat, lon, now))
        con.executemany("""INSERT INTO nodes
            (node_id,num,long_name,short_name,hw,role,last_heard,snr,hops,battery,voltage,has_env,lat,lon,updated)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(node_id) DO UPDATE SET
              long_name=excluded.long_name, role=excluded.role, last_heard=excluded.last_heard,
              hops=excluded.hops, lat=excluded.lat, lon=excluded.lon, updated=excluded.updated""", rows)
        tracked = {r[0] for r in con.execute("SELECT node_id FROM nodes WHERE tracked=1")}
        for row in rows:
            nid, lat, lon = row[0], row[12], row[13]
            if nid in tracked and lat is not None and lon is not None:
                last = con.execute("SELECT lat,lon FROM positions WHERE node_id=? ORDER BY ts DESC LIMIT 1", (nid,)).fetchone()
                if (not last) or round(last[0], 6) != round(lat, 6) or round(last[1], 6) != round(lon, 6):
                    con.execute("INSERT INTO positions(node_id,ts,lat,lon,alt) VALUES(?,?,?,?,NULL)", (nid, row[6] or now, lat, lon))
        con.execute("DELETE FROM positions WHERE ts < ?", (now - 31 * 86400,))
        con.commit()
        set_meta(con, "contacts_count", len(rows))
        withpos = sum(1 for r in rows if r[12] is not None)
        log(f"nodi MeshCore aggiornati: {len(rows)} ({withpos} con posizione)")
    except Exception as e:
        log("dump_nodes err:", e)

async def process_mc_commands(mc, con):
    """Esegue i comandi MeshCore accodati dal web (advert, ping) e riscrive il risultato.
    Estensibile: nuove azioni = nuovi rami qui + endpoint web."""
    try:
        pend = con.execute("SELECT id,action,params FROM mc_commands WHERE status='pending' ORDER BY id LIMIT 3").fetchall()
    except Exception:
        return
    for cid, action, params_json in pend:
        try:
            params = json.loads(params_json) if params_json else {}
        except Exception:
            params = {}
        try:
            if action == "advert":
                flood = bool(params.get("flood"))
                await _maybe(mc.commands.send_advert(flood=flood))
                res = {"ok": True, "flood": flood}
            elif action == "ping":
                nid = params.get("node_id")
                contact = None
                try:
                    contact = mc.get_contact_by_key_prefix(nid)
                except Exception:
                    contact = None
                if not contact:
                    raise RuntimeError("contatto non trovato")
                ctype = contact.get("type")
                if ctype == 1:
                    # companion: NON è un vero ping → invia msg 'ping' e attende l'ACK di consegna
                    got = {"ok": False, "trip": None}
                    target = {"code": None}
                    def on_ack(ev):
                        pl = getattr(ev, "payload", None) or {}
                        code = pl.get("code") if isinstance(pl, dict) else None
                        if target["code"] is None or code == target["code"]:
                            got["ok"] = True
                            got["trip"] = pl.get("trip_time") if isinstance(pl, dict) else None
                    sub = mc.subscribe(EventType.ACK, on_ack)
                    t0 = time.time()
                    try:
                        ev = await _maybe(mc.commands.send_msg(contact, "ping"))
                        pl = getattr(ev, "payload", None) or {}
                        ea = pl.get("expected_ack") if isinstance(pl, dict) else None
                        target["code"] = ea.hex() if isinstance(ea, (bytes, bytearray)) else (str(ea) if ea else None)
                        while time.time() - t0 < 15 and not got["ok"]:
                            await asyncio.sleep(0.25)
                    finally:
                        try:
                            mc.unsubscribe(sub)
                        except Exception:
                            try: sub.unsubscribe()
                            except Exception: pass
                    secs = round(got["trip"] / 1000.0, 2) if got["trip"] else round(time.time() - t0, 1)
                    res = {"method": "dm", "reachable": got["ok"], "seconds": secs,
                           "info": ("consegnato (ACK)" if got["ok"] else "nessun ACK")}
                else:
                    # ripetitore/room: path discovery (silenzioso)
                    t0 = time.time()
                    ev = await _maybe(mc.commands.send_path_discovery_sync(contact, timeout=25))
                    dt = round(time.time() - t0, 1)
                    reachable = bool(ev) and (getattr(ev, "type", None) is not None) and (not _is_err(ev))
                    res = {"method": "path", "reachable": reachable, "seconds": dt,
                           "info": ("percorso trovato" if reachable else "nessuna risposta")}
            elif action == "prune":
                res = await prune_contacts(mc, con, int(params.get("days") or 0))
            else:
                raise RuntimeError("azione sconosciuta: " + str(action))
            con.execute("UPDATE mc_commands SET status='done', result=?, done_ts=? WHERE id=?",
                        (json.dumps(res), int(time.time()), cid))
            con.commit()
            log(f"CMD #{cid} {action} -> {res}")
        except Exception as e:
            con.execute("UPDATE mc_commands SET status='error', error=?, done_ts=? WHERE id=?",
                        (str(e), int(time.time()), cid))
            con.commit()
            log(f"CMD #{cid} {action} ERR: {e}")

def get_meta_int(con, key, default=0):
    try:
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row[0]) if row and row[0] else default
    except Exception:
        return default

def set_meta(con, key, value):
    try:
        con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))
        con.commit()
    except Exception:
        pass

async def prune_contacts(mc, con, days):
    """Libera posti nella rubrica del nodo (limite firmware `max_contacts`, es. 350:
    quando è piena il nodo NON registra più nuovi contatti). Rimuove dal nodo i
    contatti non più sentiti da `days` giorni; i nodi tracciati restano, e lo storico
    nella tabella `nodes` non viene toccato."""
    if days <= 0:
        return {"removed": 0, "reason": "giorni non validi"}
    await asyncio.wait_for(_maybe(mc.commands.get_contacts()), CALL_TIMEOUT)
    cs = getattr(mc, "contacts", None) or {}
    items = list(cs.values()) if isinstance(cs, dict) else list(cs)
    keep = {r[0] for r in con.execute("SELECT node_id FROM nodes WHERE tracked=1")}
    cutoff = int(time.time()) - days * 86400
    removed = errors = 0
    for v in items:
        if not isinstance(v, dict):
            continue
        pk = v.get("public_key") or ""
        nid = pk[:12]
        if not pk or nid in keep:
            continue
        if (v.get("last_advert") or 0) >= cutoff:
            continue
        try:
            ev = await asyncio.wait_for(_maybe(mc.commands.remove_contact(pk)), CALL_TIMEOUT)
            if _is_err(ev):
                errors += 1
            else:
                removed += 1
                # la libreria non toglie mai nulla dalla sua cache dei contatti
                # (get_contacts fa solo merge): la allineo a mano, altrimenti il
                # conteggio resta fermo al valore pre-pulizia.
                if isinstance(cs, dict):
                    cs.pop(pk, None)
        except Exception:
            errors += 1
        _progress()   # la pulizia può essere lunga: tiene buono il watchdog
    remaining = len(getattr(mc, "contacts", None) or {})
    set_meta(con, "contacts_count", remaining)
    log(f"rubrica nodo: rimossi {removed} contatti più vecchi di {days}g (restano {remaining}, errori {errors})")
    return {"removed": removed, "errors": errors, "remaining": remaining, "days": days}

async def dump_channels_mc(mc, con):
    """Interroga i canali MeshCore e li salva nella tabella `channels` (idx,name,role)
    compatibile con /api/channels. Un canale è attivo se ha nome o un secret non-nullo."""
    try:
        rows = []
        for idx in range(NUM_CHANNELS):
            ev = await _maybe(mc.commands.get_channel(idx))
            if ev is None or _is_err(ev):
                continue
            pl = getattr(ev, "payload", ev)
            if not isinstance(pl, dict):
                continue
            name = pl.get("channel_name", "") or ""
            secret = pl.get("channel_secret", b"") or b""
            enabled = bool(name) or (isinstance(secret, (bytes, bytearray)) and any(secret))
            if not enabled:
                continue
            role = 1 if idx == 0 else 2
            rows.append((idx, name or (f"Canale {idx}"), role))
        con.execute("DELETE FROM channels")
        con.executemany("INSERT OR REPLACE INTO channels(idx,name,role) VALUES(?,?,?)", rows)
        con.commit()
        log(f"canali MeshCore aggiornati: {len(rows)} attivi ({', '.join(r[1] for r in rows)})")
    except Exception as e:
        log("dump_channels err:", e)

async def make_mc():
    if CONN == "serial":
        mc = await _maybe(MeshCore.create_serial(PORT, baudrate=BAUD, auto_reconnect=True))
    elif CONN == "tcp":
        host = await asyncio.get_event_loop().run_in_executor(None, resolve_tcp_host)
        mc = await _maybe(MeshCore.create_tcp(host, TCP_PORT, auto_reconnect=True))
    elif CONN == "ble":
        mc = await _maybe(MeshCore.create_ble(address=BLE_ADDR, pin=BLE_PIN, auto_reconnect=True))
    else:
        raise ValueError("MC_CONN non valido: " + CONN)
    if not getattr(mc, "is_connected", False):
        await _maybe(mc.connect())
    return mc

def _is_err(ev):
    f = getattr(ev, "is_error", None)
    try:
        return f() if callable(f) else bool(f)
    except Exception:
        return False

async def _one_request(mc):
    if SENSOR == "self":
        return await _maybe(mc.commands.get_self_telemetry())
    await _maybe(mc.ensure_contacts())
    contact = mc.get_contact_by_name(SENSOR) or mc.get_contact_by_key_prefix(SENSOR)
    if not contact:
        log("contatto sensore non trovato:", SENSOR)
        return None
    return await _maybe(mc.commands.req_telemetry(contact))

async def get_telemetry(mc):
    """Interroga la telemetria; ritenta perché il canale ambientale (BME) a volte
    manca nella risposta. Ritorna dict con temp/umidità/pressione o {}."""
    last_payload = None
    for attempt in range(4):
        try:
            ev = await asyncio.wait_for(_one_request(mc), CALL_TIMEOUT)
        except asyncio.TimeoutError:
            log("telemetria: timeout richiesta al nodo"); ev = None
        if ev is None or _is_err(ev):
            await asyncio.sleep(1.5); continue
        last_payload = getattr(ev, "payload", ev)
        m = parse_lpp(last_payload)
        if m.get("humidity") is not None or m.get("pressure") is not None:
            return m
        await asyncio.sleep(1.5)
    log("nessun canale ambientale nella risposta; ultimo payload:", repr(last_payload)[:250])
    return {}

async def main():
    con = init_db()
    log(f"logger MeshCore avviato: conn={CONN} sensor={SENSOR} interval={INTERVAL}s")
    asyncio.create_task(_watchdog())
    mc = None
    myname = "MeshCore"
    last_nodes = 0
    last_auto_advert = 0
    last_auto_prune = 0
    last_ok = time.time()
    while True:
        cycle = time.time()
        _progress()
        try:
            # "connesso" ma senza dati salvati da troppo tempo (socket half-open / nodo muto)
            # -> forza una riconnessione completa (che ri-scopre anche l'IP del nodo)
            if mc is not None and getattr(mc, "is_connected", False) and (time.time() - last_ok) > STALE_RECONNECT:
                log(f"nessun dato salvato da {int(time.time()-last_ok)}s: riconnessione forzata")
                try: await asyncio.wait_for(_maybe(mc.disconnect()), 10)
                except Exception: pass
                mc = None
            if mc is None or not getattr(mc, "is_connected", False):
                log("connessione...")
                mc = await asyncio.wait_for(make_mc(), CONNECT_TIMEOUT)
                try:
                    si = getattr(mc, "self_info", None) or {}
                    myname = si.get("name") or myname
                except Exception:
                    pass
                try:   # limite di rubrica del firmware: serve a segnalare quando è piena
                    ev = await asyncio.wait_for(_maybe(mc.commands.send_device_query()), CALL_TIMEOUT)
                    pl = getattr(ev, "payload", None) or {}
                    if isinstance(pl, dict) and pl.get("max_contacts"):
                        set_meta(con, "max_contacts", pl["max_contacts"])
                except Exception as e:
                    log("device_query err:", e)
                await dump_channels_mc(mc, con)
                await dump_nodes_mc(mc, con); last_nodes = time.time()
                try:
                    await _maybe(mc.set_decrypt_channel_logs(True))  # attacca path/SNR ai msg di canale
                except Exception as e:
                    log("decrypt_channel_logs err:", e)
                try:
                    mc.subscribe(EventType.CHANNEL_MSG_RECV, on_chan_msg)
                    mc.subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)
                    mc.subscribe(EventType.RX_LOG_DATA, on_rx_log)
                    await _maybe(mc.start_auto_message_fetching())
                    log("chat canali attiva")
                except Exception as e:
                    log("subscribe/fetch err:", e)
                log(f"connesso (nodo: {myname})")
                last_ok = time.time()
            await process_outbox(mc, con, myname)
            await process_mc_commands(mc, con)
            # advertise automatico (configurabile da /api/mc/config)
            aam = get_meta_int(con, "auto_advert_min")
            if aam > 0 and (time.time() - last_auto_advert) >= aam * 60:
                try:
                    await _maybe(mc.commands.send_advert(flood=False))
                    last_auto_advert = time.time()
                    log(f"AUTO-ADVERT inviato (ogni {aam} min)")
                except Exception as e:
                    log("auto-advert err:", e)
            # pulizia automatica della rubrica: tiene posti liberi per i nuovi nodi
            apd = get_meta_int(con, "auto_prune_days")
            if apd > 0 and (time.time() - last_auto_prune) >= 3600:
                try:
                    await prune_contacts(mc, con, apd)
                except Exception as e:
                    log("auto-prune err:", e)
                last_auto_prune = time.time()
            if time.time() - last_nodes >= NODES_REFRESH:
                await dump_nodes_mc(mc, con); last_nodes = time.time()
            m = await get_telemetry(mc)
            if m and not plausible(m):
                log(f"lettura fuori scala, scartata: {m}")
            elif m and any(m.get(k) is not None for k in ("temperature", "humidity", "pressure")):
                save(con, m)
                last_ok = time.time()
                log(f"SAVED T={m.get('temperature')} RH={m.get('humidity')} P={m.get('pressure')}")
            else:
                log("nessuna telemetria in questo ciclo")
        except Exception as e:
            log("errore/riconnessione:", e)
            try:
                if mc: await _maybe(mc.disconnect())
            except Exception:
                pass
            mc = None
            await asyncio.sleep(5)
        await asyncio.sleep(max(1, INTERVAL - (time.time() - cycle)))

if __name__ == "__main__":
    asyncio.run(main())
