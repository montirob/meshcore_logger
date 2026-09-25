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
TXT_CLI_DATA = 1  # txt_type delle risposte ai comandi CLI inviati a ripetitori/room
RPT_TIMEOUT = int(os.environ.get("MC_RPT_TIMEOUT", "45"))   # s max per una richiesta di gestione remota (multi-hop)
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
# "Eco" dei nostri advert: quando un ripetitore ritrasmette il nostro advert e il nodo
# lo risente, l'RX log mostra cosa è andato davvero in onda (posizione sì/no, salti).
_self_pk = ""       # chiave pubblica del nodo locale (hex minuscolo), nota dopo la connessione
_adv_echoes = []    # ultimi advert propri risentiti

def _on_self_advert(pl):
    e = {"t": int(time.time()), "has_loc": pl.get("adv_lat") is not None,
         "lat": pl.get("adv_lat"), "lon": pl.get("adv_lon"), "name": pl.get("adv_name"),
         "adv_ts": pl.get("adv_timestamp"), "hops": pl.get("path_len"), "path": pl.get("path"),
         "route": pl.get("route_typename"), "snr": pl.get("snr"), "rssi": pl.get("rssi")}
    _adv_echoes.append(e)
    del _adv_echoes[:-20]
    try:
        con = sqlite3.connect(DB)
        con.execute("PRAGMA busy_timeout=8000")
        con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('self_adv_echo',?)", (json.dumps(e),))
        con.commit(); con.close()
    except Exception:
        pass
    log(f"ECO advert proprio: posizione={'sì' if e['has_loc'] else 'NO'} {e['lat']},{e['lon']} salti={e['hops']}")

def on_rx_log(ev):
    try:
        pl = getattr(ev, "payload", None) or {}
        if not isinstance(pl, dict):
            return
        if pl.get("payload_typename") == "ADVERT":
            if _self_pk and (pl.get("adv_key") or "").lower() == _self_pk:
                _on_self_advert(pl)
            return
        if pl.get("payload_typename") != "GRP_TXT":
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
        if pl.get("txt_type") == TXT_CLI_DATA:
            return   # risposta a un comando CLI di gestione: la raccoglie rpt_action, non è un DM
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

# ---------------- Gestione remota di ripetitori / room ----------------
# Il companion inoltra le richieste al nodo remoto via mesh. Serve prima un login
# (password admin o guest) con send_login: il ripetitore ricorda la sessione per un
# po', poi le richieste binarie (status, telemetria, vicini, ACL) e i comandi CLI
# (`get`/`set`, `reboot`, `clock sync`, …) vengono accettati. Owner e regioni sono
# richieste anonime: funzionano anche senza login.
RPT_OPS = ("login", "logout", "status", "telemetry", "neighbours", "acl", "owner", "regions", "cli")
_SENSITIVE_CLI = ("password ", "set guest.password")

def _redact(params):
    """Copia dei parametri senza segreti (password di login o nei comandi CLI)."""
    p = dict(params or {})
    if "password" in p:
        p["password"] = "***"
    cmd = str(p.get("cmd") or "")
    for s in _SENSITIVE_CLI:
        if cmd.lower().startswith(s):
            p["cmd"] = cmd[:len(s)] + "***"
    return p

def _suggested_wait(sent_payload, floor=10):
    """Attesa della risposta dalla mesh: la stima del firmware (ms), con margine."""
    st = (sent_payload or {}).get("suggested_timeout") if isinstance(sent_payload, dict) else None
    t = (st / 800.0) if st else 20
    return max(floor, min(RPT_TIMEOUT, t))

async def _send_and_wait(mc, send, event_types, match, floor=10):
    """Invia una richiesta e attende il primo evento (tra `event_types`) per cui
    `match(payload)` è vero. Mi iscrivo PRIMA di inviare per non perdere risposte
    rapide. Ritorna l'evento o None se il nodo remoto non risponde."""
    fut = asyncio.get_running_loop().create_future()
    def cb(ev):
        pl = getattr(ev, "payload", None) or {}
        if not fut.done() and isinstance(pl, dict) and match(pl):
            fut.set_result(ev)
    subs = [mc.subscribe(t, cb) for t in event_types]
    try:
        sent = await asyncio.wait_for(_maybe(send()), CALL_TIMEOUT)
        if sent is None or _is_err(sent):
            reason = (getattr(sent, "payload", None) or {}) if sent is not None else {}
            raise RuntimeError("il nodo locale non ha inviato la richiesta: " + str(reason.get("reason") or reason or "?"))
        try:
            return await asyncio.wait_for(fut, _suggested_wait(getattr(sent, "payload", None), floor))
        except asyncio.TimeoutError:
            return None
    finally:
        for s in subs:
            try: s.unsubscribe()
            except Exception: pass

def _contact_names(mc):
    """[(public_key hex minuscolo, nome)] dei contatti noti: per dare un nome ai
    prefissi di chiave restituiti da vicini/ACL."""
    cs = getattr(mc, "contacts", None) or {}
    out = []
    for k, v in (cs.items() if isinstance(cs, dict) else []):
        if isinstance(v, dict):
            out.append(((v.get("public_key") or str(k)).lower(), v.get("adv_name")))
    return out

def _name_for(names, prefix):
    prefix = (prefix or "").lower()
    hits = [n for pk, n in names if prefix and pk.startswith(prefix)]
    return hits[0] if len(hits) == 1 else None

NO_REPLY = "nessuna risposta (nodo fuori portata o sessione scaduta: rifai il login)"

async def rpt_action(mc, params):
    """Esegue un'operazione di gestione remota su un ripetitore/room.
    params: {node_id, op, password? (login), cmd? (cli)}. Ritorna {op, ok, info, data}."""
    op = params.get("op")
    if op not in RPT_OPS:
        raise RuntimeError("operazione sconosciuta: " + str(op))
    contact = mc.get_contact_by_key_prefix(params.get("node_id") or "")
    if not contact:
        raise RuntimeError("contatto non trovato nella rubrica del nodo")
    prefix = (contact.get("public_key") or "")[:12].lower()
    same_node = lambda pl: (pl.get("pubkey_prefix") or prefix).lower().startswith(prefix)

    if op == "login":
        pwd = str(params.get("password") or "")
        cmds = mc.commands
        send = (lambda: cmds._send_login_raw(contact, pwd)) if hasattr(cmds, "_send_login_raw") \
            else (lambda: cmds.send_login(contact, pwd))
        ev = await _send_and_wait(mc, send, [EventType.LOGIN_SUCCESS, EventType.LOGIN_FAILED], same_node, floor=15)
        if ev is None:
            return {"op": op, "ok": False, "info": "nessuna risposta al login (nodo fuori portata?)"}
        if ev.type == EventType.LOGIN_FAILED:
            return {"op": op, "ok": False, "info": "password errata"}
        pl = ev.payload or {}
        return {"op": op, "ok": True, "info": "accesso " + ("amministratore" if pl.get("is_admin") else "ospite"),
                "data": {k: pl.get(k) for k in ("is_admin", "permissions", "acl_permissions", "server_timestamp", "fw_ver_level")}}

    if op == "logout":
        await asyncio.wait_for(_maybe(mc.commands.send_logout(contact)), CALL_TIMEOUT)
        return {"op": op, "ok": True, "info": "disconnesso"}

    if op == "cli":
        cmd = str(params.get("cmd") or "").strip()
        if not cmd:
            raise RuntimeError("comando vuoto")
        ev = await _send_and_wait(mc, lambda: mc.commands.send_cmd(contact, cmd),
                                  [EventType.CONTACT_MSG_RECV],
                                  lambda pl: pl.get("txt_type") == TXT_CLI_DATA and same_node(pl), floor=15)
        shown = _redact({"cmd": cmd})["cmd"]
        if ev is None:
            return {"op": op, "ok": False, "info": NO_REPLY, "data": {"cmd": shown}}
        reply = (ev.payload or {}).get("text", "")
        if shown != cmd:   # il firmware ripete la nuova password nella risposta
            secret = cmd.split(None, 2)[-1] if cmd.lower().startswith("set ") else cmd.split(None, 1)[-1]
            reply = reply.replace(secret, "***")
        return {"op": op, "ok": True, "info": "risposta ricevuta", "data": {"cmd": shown, "reply": reply}}

    # richieste binarie/anonime: la libreria attende già la risposta (…_sync)
    c = mc.commands
    call = {"status":     lambda: c.req_status_sync(contact),
            "telemetry":  lambda: c.req_telemetry_sync(contact),
            "neighbours": lambda: c.fetch_all_neighbours(contact),
            "acl":        lambda: c.req_acl_sync(contact),
            "owner":      lambda: c.req_owner_sync(contact),
            "regions":    lambda: c.req_regions_sync(contact)}[op]
    try:
        data = await asyncio.wait_for(_maybe(call()), RPT_TIMEOUT)
    except asyncio.TimeoutError:
        data = None
    if data is None:
        return {"op": op, "ok": False, "info": NO_REPLY}
    names = _contact_names(mc)
    if op == "telemetry":
        data = _lpp_items(data)
    elif op == "neighbours":
        data = {"total": data.get("neighbours_count"),
                "items": [dict(n, name=_name_for(names, n.get("pubkey"))) for n in (data.get("neighbours") or [])]}
    elif op == "acl":
        data = [dict(a, name=_name_for(names, a.get("key"))) for a in (data or [])]
    elif op == "status":
        data = {k: v for k, v in data.items() if k != "tag"}
    return {"op": op, "ok": True, "info": "ok", "data": data}

# ---------------- Configurazione del nodo locale (companion collegato al Pi) ----------------
# Tutto passa dal protocollo companion (niente mesh): risposte immediate.
# Codici firmware: telemetria 0=negata, 1=solo contatti con permesso, 2=tutti;
# autoadd_config = bitmask AUTO_ADD_* (bit0 = sovrascrivi i più vecchi a rubrica piena).
SELF_OTHER_KEYS = ("telemetry_mode_base", "telemetry_mode_loc", "telemetry_mode_env",
                   "adv_loc_policy", "multi_acks", "manual_add_contacts")

async def _ask(fn):
    """Payload della risposta del nodo, o None (errore, timeout, comando non supportato)."""
    try:
        ev = await asyncio.wait_for(_maybe(fn()), CALL_TIMEOUT)
    except Exception:
        return None
    if ev is None or _is_err(ev):
        return None
    return getattr(ev, "payload", None)

async def self_read(mc, con):
    """Legge configurazione, stato e statistiche del nodo locale. Salva l'ultima
    lettura in meta (`self_state`) così la pagina la mostra subito all'apertura."""
    c = mc.commands
    st = {"ts": int(time.time())}
    st["info"] = await _ask(c.send_appstart) or {}
    st["device"] = await _ask(c.send_device_query) or {}
    st["battery"] = await _ask(c.get_bat)
    t = await _ask(c.get_time)
    st["clock"] = {"node": t.get("time"), "pi": int(time.time())} if t else None
    tun = await _ask(c.get_tuning)
    st["tuning"] = {"rx_delay": tun["rx_delay"] / 1000.0, "af": tun["airtime_factor"] / 1000.0} if tun else None
    st["autoadd"] = await _ask(c.get_autoadd_config)
    st["custom_vars"] = await _ask(c.get_custom_vars)
    rf = await _ask(c.get_allowed_repeat_freq)
    st["repeat_freqs"] = (rf or {}).get("freqs")
    st["stats"] = {"core": await _ask(c.get_stats_core),
                   "radio": await _ask(c.get_stats_radio),
                   "packets": await _ask(c.get_stats_packets)}
    tel = await _ask(c.get_self_telemetry)
    st["telemetry"] = _lpp_items(tel) if tel else None
    info = st["info"]
    if info:
        set_meta(con, "self_name", info.get("name") or "")
        if info.get("adv_lat") or info.get("adv_lon"):
            set_meta(con, "self_lat", info.get("adv_lat"))
            set_meta(con, "self_lon", info.get("adv_lon"))
    set_meta(con, "self_state", json.dumps(st, default=str))
    return st

async def self_set(mc, con, ch):
    """Applica le modifiche `ch` (solo le chiavi presenti) e rilegge lo stato.
    Ritorna {applied: {chiave: "ok" | motivo}, state}."""
    c = mc.commands
    applied = {}
    async def step(key, fn):
        try:
            ev = await asyncio.wait_for(_maybe(fn()), CALL_TIMEOUT)
        except Exception as e:
            applied[key] = "errore: " + (str(e) or type(e).__name__); return
        if ev is None or _is_err(ev):
            pl = (getattr(ev, "payload", None) or {}) if ev is not None else {}
            applied[key] = "rifiutato dal nodo" + (f" ({pl.get('code_string') or pl.get('error_code') or pl.get('reason')})" if pl else "")
        else:
            applied[key] = "ok"
    if "name" in ch:
        await step("name", lambda: c.set_name(str(ch["name"])))
    if "lat" in ch and "lon" in ch:
        await step("coords", lambda: c.set_coords(float(ch["lat"]), float(ch["lon"])))
    if "tx_power" in ch:
        await step("tx_power", lambda: c.set_tx_power(int(ch["tx_power"])))
    if "radio" in ch:
        r = ch["radio"]
        await step("radio", lambda: c.set_radio(float(r["freq"]), float(r["bw"]), int(r["sf"]), int(r["cr"]),
                                                 (int(bool(r["repeat"])) if r.get("repeat") is not None else None)))
    if "tuning" in ch:
        tu = ch["tuning"]   # il firmware li vuole moltiplicati per 1000
        await step("tuning", lambda: c.set_tuning(int(round(float(tu["rx_delay"]) * 1000)), int(round(float(tu["af"]) * 1000))))
    if any(k in ch for k in SELF_OTHER_KEYS):
        # un unico comando porta tutti questi campi: parto dai valori attuali
        async def other():
            ev = await _maybe(c.send_appstart())
            if ev is None or _is_err(ev):
                return ev
            infos = dict(ev.payload)
            for k in SELF_OTHER_KEYS:
                if k in ch:
                    infos[k] = bool(ch[k]) if k == "manual_add_contacts" else int(ch[k])
            return await _maybe(c.set_other_params_from_infos(infos))
        await step("other", other)
    if "autoadd" in ch:
        a = ch["autoadd"]   # la libreria non manda max_hops: frame scritto a mano
        frame = bytes([0x3A, int(a["config"]) & 0xFF]) + (bytes([int(a["max_hops"])]) if a.get("max_hops") is not None else b"")
        await step("autoadd", lambda: c.send(frame, [EventType.OK, EventType.ERROR]))
    if "path_hash_mode" in ch:
        await step("path_hash_mode", lambda: c.set_path_hash_mode(int(ch["path_hash_mode"])))
    for k, v in (ch.get("custom_vars") or {}).items():
        await step("var " + k, lambda k=k, v=v: c.set_custom_var(str(k), str(v)))
    log(f"nodo locale: modifiche {applied}")
    return {"applied": applied, "state": await self_read(mc, con)}

# ---------------- Console del nodo locale ----------------
# Il companion non ha una CLI testuale (parla solo il protocollo binario): questi
# comandi sono tradotti nelle chiamate della libreria.
SELF_CLI_HELP = """comandi:
  info                   nome, chiave, posizione, radio, opzioni
  ver                    modello e firmware
  bat                    batteria e memoria
  clock [sync]           orologio del nodo (sync = allinea al Raspberry)
  tele                   telemetria del nodo (LPP)
  stats                  statistiche core / radio / pacchetti
  contacts [testo]       contatti in rubrica (filtrati per nome)
  advert [flood]         invia un advert (zero-hop, oppure flood)
  advcheck               advert flood + ascolto dell'eco: mostra cosa è andato in onda
  echo                   ultimi advert propri risentiti dalla rete
  get <chiave>           name · coords · advloc · tx · radio · telemetry · autoadd · tuning · vars · phm · multiacks
  set name <nome>
  set coords <lat> <lon>
  set advloc on|off
  set tx <dBm>
  set telemetry base|loc|env 0|1|2   (0 negata · 1 contatti autorizzati · 2 tutti)
  set multiacks on|off
  set var <nome> <valore>
  reboot"""
TELEM_LBL = {0: "negata", 1: "solo contatti autorizzati", 2: "tutti"}
ADVERT_ECHO_WAIT = 30   # s di ascolto dell'eco dopo un advert flood

def _onoff(v):
    v = str(v).lower()
    if v in ("on", "1", "si", "sì", "yes", "true"):
        return 1
    if v in ("off", "0", "no", "false"):
        return 0
    raise RuntimeError("valore atteso: on/off")

def _fmt_echo(e):
    when = datetime.datetime.fromtimestamp(e["t"]).strftime("%d/%m %H:%M:%S")
    pos = f"posizione {e['lat']}, {e['lon']}" if e.get("has_loc") else "SENZA posizione"
    return (f"{when}  {pos} · nome {e.get('name') or '—'} · salti {e.get('hops')}"
            f"{' (' + e['path'] + ')' if e.get('path') else ''} · SNR {e.get('snr')}")

def _fmt_self_info(i, dv):
    loc = "sì" if i.get("adv_loc_policy") else "NO"
    return "\n".join([
        f"nome        {i.get('name')}",
        f"chiave      {i.get('public_key')}",
        f"posizione   {i.get('adv_lat')}, {i.get('adv_lon')}   (negli advert: {loc}, policy={i.get('adv_loc_policy')})",
        f"radio       {i.get('radio_freq')} MHz · BW {i.get('radio_bw')} kHz · SF{i.get('radio_sf')} · CR 4/{i.get('radio_cr')}"
        f" · TX {i.get('tx_power')}/{i.get('max_tx_power')} dBm",
        "telemetria  " + " · ".join(f"{k} {TELEM_LBL.get(i.get('telemetry_mode_' + k), i.get('telemetry_mode_' + k))}"
                                    for k in ("base", "loc", "env")),
        f"contatti    aggiunta {'manuale/per tipo' if i.get('manual_add_contacts') else 'automatica'} · multi-ACK {i.get('multi_acks')}",
        f"firmware    {dv.get('model')} {dv.get('ver')} ({dv.get('fw_build')}) · repeat {dv.get('repeat')} · hash percorso {dv.get('path_hash_mode')}",
    ])

async def self_cli(mc, con, line):
    """Esegue un comando della console del nodo locale. Ritorna {reply, state?}
    (state quando il comando ha cambiato la configurazione)."""
    global _self_pk
    c = mc.commands
    args = line.split()
    if not args:
        raise RuntimeError("comando vuoto")
    cmd, rest = args[0].lower(), args[1:]

    if cmd in ("help", "?", "aiuto"):
        return {"reply": SELF_CLI_HELP}
    if cmd == "info":
        i = await _ask(c.send_appstart) or {}
        dv = await _ask(c.send_device_query) or {}
        _self_pk = (i.get("public_key") or _self_pk).lower()
        return {"reply": _fmt_self_info(i, dv)}
    if cmd == "ver":
        dv = await _ask(c.send_device_query) or {}
        return {"reply": "\n".join(f"{k}: {v}" for k, v in dv.items())}
    if cmd == "bat":
        b = await _ask(c.get_bat) or {}
        return {"reply": f"batteria {b.get('level', 0) / 1000:.2f} V · memoria {b.get('used_kb')}/{b.get('total_kb')} kB"}
    if cmd == "clock":
        if rest[:1] == ["sync"]:
            await self_action(mc, con, {"op": "time_sync"})
        t = await _ask(c.get_time) or {}
        node, now = t.get("time"), int(time.time())
        if not node:
            return {"reply": "orologio non disponibile"}
        return {"reply": f"nodo {datetime.datetime.fromtimestamp(node)} · Raspberry {datetime.datetime.fromtimestamp(now)}"
                         f" · scarto {node - now:+d} s"}
    if cmd in ("tele", "telemetry"):
        tel = await _ask(c.get_self_telemetry)
        items = _lpp_items(tel) if tel else []
        return {"reply": "\n".join(f"ch{x.get('channel')} {x.get('type')}: {x.get('value')}" for x in items) or "nessuna telemetria"}
    if cmd == "stats":
        out = []
        for name, fn in (("core", c.get_stats_core), ("radio", c.get_stats_radio), ("pacchetti", c.get_stats_packets)):
            s = await _ask(fn) or {}
            out.append(name + ": " + " · ".join(f"{k} {v}" for k, v in s.items()))
        return {"reply": "\n".join(out)}
    if cmd == "contacts":
        await asyncio.wait_for(_maybe(c.get_contacts()), CALL_TIMEOUT)
        cs = [v for v in (getattr(mc, "contacts", None) or {}).values() if isinstance(v, dict)]
        dv = await _ask(c.send_device_query) or {}
        head = f"{len(cs)} contatti in rubrica (max {dv.get('max_contacts', '?')})"
        if not rest:
            return {"reply": head}
        q = " ".join(rest).lower()
        hit = [v for v in cs if q in (v.get("adv_name") or "").lower()][:30]
        rows = [f"{(v.get('public_key') or '')[:12]}  {v.get('adv_name')}  tipo {v.get('type')}"
                f"  pos {v.get('adv_lat')},{v.get('adv_lon')}  ultimo advert "
                f"{datetime.datetime.fromtimestamp(v['last_advert']) if v.get('last_advert') else '—'}" for v in hit]
        return {"reply": head + "\n" + ("\n".join(rows) or "nessun contatto con quel nome")}
    if cmd == "advert":
        flood = rest[:1] == ["flood"]
        await asyncio.wait_for(_maybe(c.send_advert(flood=flood)), CALL_TIMEOUT)
        return {"reply": f"advert {'flood' if flood else 'zero-hop'} inviato"}
    if cmd == "advcheck":
        i = await _ask(c.send_appstart) or {}
        _self_pk = (i.get("public_key") or _self_pk).lower()
        pre = (f"impostazione: posizione negli advert {'SÌ' if i.get('adv_loc_policy') else 'NO'}"
               f" · coordinate {i.get('adv_lat')}, {i.get('adv_lon')}")
        t0 = time.time()
        await asyncio.wait_for(_maybe(c.send_advert(flood=True)), CALL_TIMEOUT)
        first = None
        while time.time() - t0 < ADVERT_ECHO_WAIT:
            got = [e for e in _adv_echoes if e["t"] >= int(t0)]
            if got and first is None:
                first = time.time()
            if first and time.time() - first > 4:   # qualche secondo per altre ritrasmissioni
                break
            await asyncio.sleep(0.5)
        got = [e for e in _adv_echoes if e["t"] >= int(t0)]
        if not got:
            return {"reply": pre + f"\nadvert flood inviato; nessuna ritrasmissione sentita in {ADVERT_ECHO_WAIT} s"
                                   " (nessun ripetitore in portata l'ha ripetuto, o non l'abbiamo risentito)"}
        return {"reply": pre + f"\nadvert flood inviato; sentite {len(got)} ritrasmissioni:\n" +
                         "\n".join(_fmt_echo(e) for e in got)}
    if cmd == "echo":
        if not _adv_echoes:
            row = con.execute("SELECT value FROM meta WHERE key='self_adv_echo'").fetchone()
            if row and row[0]:
                return {"reply": "ultimo advert proprio risentito:\n" + _fmt_echo(json.loads(row[0]))}
            return {"reply": "nessun advert proprio risentito finora (prova: advcheck)"}
        return {"reply": "\n".join(_fmt_echo(e) for e in _adv_echoes)}
    if cmd == "get":
        if not rest:
            raise RuntimeError("uso: get <chiave>")
        k = rest[0].lower()
        i = await _ask(c.send_appstart) or {}
        if k == "name":           return {"reply": str(i.get("name"))}
        if k in ("coords", "lat", "lon"): return {"reply": f"{i.get('adv_lat')} {i.get('adv_lon')}"}
        if k == "advloc":         return {"reply": f"{'on' if i.get('adv_loc_policy') else 'off'} (policy={i.get('adv_loc_policy')})"}
        if k == "tx":             return {"reply": f"{i.get('tx_power')} dBm (max {i.get('max_tx_power')})"}
        if k == "radio":          return {"reply": f"{i.get('radio_freq')} MHz · BW {i.get('radio_bw')} · SF{i.get('radio_sf')} · CR 4/{i.get('radio_cr')}"}
        if k == "telemetry":      return {"reply": " · ".join(f"{m} {i.get('telemetry_mode_' + m)}" for m in ("base", "loc", "env"))}
        if k == "multiacks":      return {"reply": str(i.get("multi_acks"))}
        if k == "autoadd":        return {"reply": json.dumps(await _ask(c.get_autoadd_config))}
        if k == "tuning":
            t = await _ask(c.get_tuning) or {}
            return {"reply": f"rx_delay {t.get('rx_delay', 0) / 1000} · airtime factor {t.get('airtime_factor', 0) / 1000}"}
        if k == "vars":           return {"reply": json.dumps(await _ask(c.get_custom_vars) or {}) }
        if k == "phm":
            dv = await _ask(c.send_device_query) or {}
            return {"reply": str(dv.get("path_hash_mode"))}
        raise RuntimeError("chiave sconosciuta: " + k)
    if cmd == "set":
        if len(rest) < 2:
            raise RuntimeError("uso: set <chiave> <valore> (help per l'elenco)")
        k, v = rest[0].lower(), rest[1:]
        try:
            if k == "name":
                ch = {"name": " ".join(v)}
            elif k == "coords":
                lat, lon = float(v[0].rstrip(",")), float(v[1])
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    raise RuntimeError("coordinate fuori range")
                ch = {"lat": lat, "lon": lon}
            elif k == "advloc":
                ch = {"adv_loc_policy": _onoff(v[0])}
            elif k == "tx":
                ch = {"tx_power": int(v[0])}
            elif k == "telemetry":
                m, val = v[0].lower(), int(v[1])
                if m not in ("base", "loc", "env") or val not in (0, 1, 2):
                    raise RuntimeError("uso: set telemetry base|loc|env 0|1|2")
                ch = {"telemetry_mode_" + m: val}
            elif k == "multiacks":
                ch = {"multi_acks": _onoff(v[0])}
            elif k == "var":
                ch = {"custom_vars": {v[0]: " ".join(v[1:])}}
            else:
                raise RuntimeError("chiave non modificabile da console: " + k)
        except (ValueError, IndexError):
            raise RuntimeError("valore non valido")
        r = await self_set(mc, con, ch)
        return {"reply": " · ".join(f"{a}: {b}" for a, b in r["applied"].items()), "state": r["state"]}
    if cmd == "reboot":
        return dict(await self_action(mc, con, {"op": "reboot"}), reply="riavvio inviato: il logger si ricollega da solo")
    raise RuntimeError(f"comando sconosciuto: {cmd} (scrivi help)")

async def self_action(mc, con, params):
    op = params.get("op")
    if op == "cli":
        return dict(await self_cli(mc, con, str(params.get("cmd") or "").strip()), op=op)
    if op == "read":
        return {"op": op, "state": await self_read(mc, con)}
    if op == "set":
        return dict(await self_set(mc, con, params.get("changes") or {}), op=op)
    if op == "time_sync":
        now = int(time.time())
        ev = await asyncio.wait_for(_maybe(mc.commands.set_time(now)), CALL_TIMEOUT)
        if ev is None or _is_err(ev):
            raise RuntimeError("il nodo ha rifiutato l'orario")
        return {"op": op, "time": now}
    if op == "reboot":
        # il nodo non risponde (si riavvia): la connessione cade e il loop si
        # riconnette da solo (timeout sulle richieste -> riconnessione)
        try:
            await asyncio.wait_for(_maybe(mc.commands.reboot()), 5)
        except Exception:
            pass
        return {"op": op, "info": "riavvio inviato"}
    raise RuntimeError("operazione sconosciuta: " + str(op))

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
            elif action == "rpt":
                # la password non deve restare nel DB: la tolgo prima di usarla
                con.execute("UPDATE mc_commands SET params=? WHERE id=?", (json.dumps(_redact(params)), cid))
                con.commit()
                res = await rpt_action(mc, params)
            elif action == "self":
                res = await self_action(mc, con, params)
            else:
                raise RuntimeError("azione sconosciuta: " + str(action))
            con.execute("UPDATE mc_commands SET status='done', result=?, done_ts=? WHERE id=?",
                        (json.dumps(res, default=str), int(time.time()), cid))
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

def _has_pending(con):
    try:
        return bool(con.execute("SELECT 1 FROM mc_commands WHERE status='pending' LIMIT 1").fetchone()
                    or con.execute("SELECT 1 FROM outbox WHERE status='pending' LIMIT 1").fetchone())
    except Exception:
        return False

async def idle_until(mc, con, myname, deadline):
    """Attesa fino al prossimo ciclo di telemetria, ma controllando ogni secondo la
    coda comandi/outbox: la gestione remota dei ripetitori e l'invio dei messaggi
    rispondono in pochi secondi invece di aspettare il ciclo successivo (fino a 60s).
    Tutto resta nello stesso task: un solo interlocutore sulla connessione al nodo."""
    await asyncio.sleep(1)
    while time.time() < deadline:
        if mc is not None and getattr(mc, "is_connected", False) and _has_pending(con):
            try:
                await process_outbox(mc, con, myname)
                await process_mc_commands(mc, con)
            except Exception as e:
                log("coda comandi err:", e)
            _progress()
        await asyncio.sleep(1)

async def main():
    global _self_pk
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
                    _self_pk = (si.get("public_key") or "").lower()
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
        await idle_until(mc, con, myname, cycle + INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
