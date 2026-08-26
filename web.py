#!/usr/bin/env python3
"""Dashboard web per i dati ambientali MeshCore (LAN)."""
import os, sqlite3, datetime, math, json
from flask import Flask, jsonify, request, send_from_directory, render_template

BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.environ.get("MESH_DB", os.path.join(BASE, "meshlogger.db"))
NODE = os.environ.get("MESH_NODE_LABEL", "Nodo sensore")
PORT = int(os.environ.get("WEB_PORT", "8080"))
API_KEY = os.environ.get("API_KEY", "").strip()   # se vuoto: /api aperto (solo LAN)
PRESSURE_OFFSET = float(os.environ.get("PRESSURE_OFFSET", "11"))  # correzione altitudine (hPa)

def padj(v):
    return round(v + PRESSURE_OFFSET, 4) if (v is not None and PRESSURE_OFFSET) else v

app = Flask(__name__, static_folder="static", template_folder="templates")

@app.before_request
def _require_key():
    """Protegge gli endpoint /api con una chiave (header X-API-Key o ?key=)."""
    if not API_KEY:
        return None
    if request.path.startswith("/api/"):
        if request.method == "OPTIONS":
            return None  # preflight CORS
        key = request.headers.get("X-API-Key") or request.args.get("key", "")
        if key != API_KEY:
            return jsonify({"error": "unauthorized"}), 401
    return None

@app.after_request
def _cors(resp):
    """CORS permissivo sugli endpoint API (accesso da altri host/browser)."""
    if request.path.startswith("/api/"):
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "X-API-Key, Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

def dew_point(t, rh):
    """Punto di rugiada (Magnus) da temperatura (C) e umidita' relativa (%)."""
    if t is None or rh is None or rh <= 0:
        return None
    b, c = 17.62, 243.12
    try:
        g = math.log(rh / 100.0) + (b * t) / (c + t)
        return round((c * g) / (b - g), 2)
    except (ValueError, ZeroDivisionError):
        return None

def q(sql, args=()):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()

@app.route("/")
def index():
    return render_template("index.html", node=NODE)

def moving_avg(rows, key, window_s):
    """Media mobile CENTRATA a finestra temporale (larghezza window_s, ossia +/- window_s/2).
    Segue la curva senza ritardo. Robusta ai buchi. rows ordinate per ts crescente."""
    n = len(rows)
    out = [None] * n
    half = window_s / 2.0
    left = 0
    right = 0
    ssum = 0.0
    scnt = 0
    for i in range(n):
        lo = rows[i]["ts"] - half
        hi = rows[i]["ts"] + half
        while right < n and rows[right]["ts"] <= hi:
            v = rows[right][key]
            if v is not None:
                ssum += v; scnt += 1
            right += 1
        while left < right and rows[left]["ts"] < lo:
            v = rows[left][key]
            if v is not None:
                ssum -= v; scnt -= 1
            left += 1
        out[i] = round(ssum / scnt, 3) if scnt > 0 else None
    return out

@app.route("/api/readings")
def readings():
    hours = request.args.get("hours", default=24, type=float)
    if hours and hours > 0:
        since = int(datetime.datetime.now().timestamp()) - int(hours * 3600)
        rows = q("SELECT ts,temperature,humidity,pressure FROM readings WHERE ts>=? ORDER BY ts", (since,))
    else:
        rows = q("SELECT ts,temperature,humidity,pressure FROM readings ORDER BY ts")
    # correzione altitudine sulla pressione
    if PRESSURE_OFFSET:
        for r in rows:
            r["pressure"] = padj(r["pressure"])
    # medie mobili calcolate a piena risoluzione, PRIMA del downsampling
    t_ma = moving_avg(rows, "temperature", 600)   # 10 minuti
    p_ma = moving_avg(rows, "pressure", 600)       # 10 minuti
    for i, r in enumerate(rows):
        r["dew"]  = dew_point(r["temperature"], r["humidity"])
        r["t_ma"] = t_ma[i]
        r["p_ma"] = p_ma[i]
    maxp = 1500
    if len(rows) > maxp:
        step = len(rows) // maxp + 1
        rows = rows[::step]
    return jsonify(rows)

@app.route("/api/latest")
def latest():
    rows = q("SELECT ts,iso,temperature,humidity,pressure FROM readings ORDER BY ts DESC LIMIT 1")
    cnt  = q("SELECT COUNT(*) c, MIN(ts) mn FROM readings")[0]
    lat = rows[0] if rows else None
    if lat:
        lat["dew"] = dew_point(lat["temperature"], lat["humidity"])
        lat["pressure"] = padj(lat["pressure"])
    return jsonify({"latest": lat, "count": cnt["c"], "since": cnt["mn"]})

@app.route("/api/messages")
def messages():
    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 500))
    channel = request.args.get("channel", type=int)
    cols = ("SELECT ts,iso,from_id,from_name,to_id,channel,msg_id,text,"
            "COALESCE(outgoing,0) outgoing,reply_id,path,path_len,snr FROM messages")
    if channel is not None:
        rows = q(cols + " WHERE channel=? ORDER BY ts DESC LIMIT ?", (channel, limit))
    else:
        rows = q(cols + " ORDER BY ts DESC LIMIT ?", (limit,))
    _resolve_paths(rows)
    return jsonify(rows)

def _resolve_paths(rows):
    """Arricchisce i messaggi con `path_nodes` [{hash,name,lat,lon}] (hash→nodo per
    prefisso pubkey) e con la posizione del mittente (`sender_lat/lon/node`)."""
    if not rows:
        return
    try:
        nodes = q("SELECT node_id,long_name,lat,lon FROM nodes WHERE node_id IS NOT NULL")
    except Exception:
        nodes = []
    idx = {}       # prefisso pubkey -> lista di record nodo
    byname = {}    # long_name -> record nodo (per il mittente)
    for nr in nodes:
        nid = (nr["node_id"] or "").lower()
        rec = {"name": nr["long_name"] or nid, "lat": nr["lat"], "lon": nr["lon"]}
        if nr["long_name"]:
            byname.setdefault(nr["long_name"], rec)
        for klen in (2, 4):
            idx.setdefault(nid[:klen], []).append(rec)
    for r in rows:
        snd = byname.get(r.get("from_name"))
        if snd and snd.get("lat") is not None:
            r["sender_lat"] = snd["lat"]; r["sender_lon"] = snd["lon"]; r["sender_node"] = snd["name"]
        ph = (r.get("path") or "").lower()
        plen = r.get("path_len") or 0
        if not ph or not plen or len(ph) % plen != 0:
            continue
        step = len(ph) // plen
        out = []
        for i in range(0, len(ph), step):
            h = ph[i:i + step]
            lst = idx.get(h)
            if lst and len(lst) == 1:
                rec = lst[0]
                out.append({"hash": h, "name": rec["name"], "lat": rec["lat"], "lon": rec["lon"]})
            else:
                out.append({"hash": h, "name": None, "lat": None, "lon": None})
        r["path_nodes"] = out

@app.route("/api/self")
def api_self():
    con = sqlite3.connect(DB)
    def g(k):
        try:
            row = con.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
            return row[0] if row else None
        except Exception:
            return None
    name, lat, lon = g("self_name"), g("self_lat"), g("self_lon")
    con.close()
    return jsonify({"name": name,
                    "lat": float(lat) if lat else None,
                    "lon": float(lon) if lon else None})

ROLE_NAMES = {0: "disabilitato", 1: "primario", 2: "secondario"}

@app.route("/api/channels")
def channels():
    rows = q("SELECT idx,name,role FROM channels WHERE role!=0 ORDER BY idx")
    try:
        prow = q("SELECT value FROM meta WHERE key='modem_preset'")
        preset = prow[0]["value"] if prow else None
    except Exception:
        preset = None
    pretty = "".join(w.capitalize() for w in preset.split("_")) if preset else None
    for r in rows:
        r["role_name"] = ROLE_NAMES.get(r["role"], str(r["role"]))
        if not r["name"]:
            if r["role"] == 1:
                r["name"] = ("Primario (" + pretty + ")") if pretty else "Primario"
            else:
                r["name"] = f"Canale {r['idx']}"
    return jsonify(rows)

@app.route("/api/nodes")
def nodes():
    limit = request.args.get("limit", default=400, type=int)
    only_pos = request.args.get("pos", default=0, type=int)
    only_tracked = request.args.get("tracked", default=0, type=int)
    conds = []
    if only_pos:
        conds.append("lat IS NOT NULL")
    if only_tracked:
        conds.append("COALESCE(tracked,0)=1")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    rows = q(f"""SELECT node_id,long_name,short_name,hw,role,last_heard,snr,hops,
                        battery,voltage,has_env,lat,lon,COALESCE(tracked,0) tracked
                 FROM nodes {where} ORDER BY last_heard DESC LIMIT ?""", (limit,))
    return jsonify(rows)

@app.route("/api/node")
def node():
    nid = request.args.get("id")
    if not nid:
        return jsonify({"error": "id mancante"}), 400
    rows = q("""SELECT node_id,num,long_name,short_name,hw,role,last_heard,snr,hops,
                       battery,voltage,has_env,lat,lon,COALESCE(tracked,0) tracked,updated
                FROM nodes WHERE node_id=?""", (nid,))
    if not rows:
        return jsonify({"error": "non trovato", "node_id": nid}), 404
    r = rows[0]
    lastpos = q("SELECT ts,lat,lon FROM positions WHERE node_id=? ORDER BY ts DESC LIMIT 1", (nid,))
    r["last_position"] = lastpos[0] if lastpos else None
    return jsonify(r)

@app.route("/api/track", methods=["POST", "OPTIONS"])
def track():
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.get_json(silent=True) or {}
    node_id = data.get("node_id")
    tracked = 1 if data.get("tracked") else 0
    if not node_id:
        return jsonify({"error": "node_id mancante"}), 400
    con = sqlite3.connect(DB)
    cur = con.execute("UPDATE nodes SET tracked=? WHERE node_id=?", (tracked, node_id))
    # se attivo il tracking, semina la posizione attuale dal nodo
    if tracked:
        r = con.execute("SELECT lat,lon,last_heard FROM nodes WHERE node_id=?", (node_id,)).fetchone()
        if r and r[0] is not None:
            has = con.execute("SELECT 1 FROM positions WHERE node_id=? LIMIT 1", (node_id,)).fetchone()
            if not has:
                con.execute("INSERT INTO positions(node_id,ts,lat,lon,alt) VALUES(?,?,?,?,NULL)",
                            (node_id, r[2] or int(datetime.datetime.now().timestamp()), r[0], r[1]))
    con.commit()
    changed = cur.rowcount
    con.close()
    return jsonify({"ok": True, "tracked": bool(tracked), "changed": changed})

@app.route("/api/positions")
def positions():
    node_id = request.args.get("node_id")
    days = request.args.get("days", default=30, type=float)
    since = int(datetime.datetime.now().timestamp()) - int(days * 86400)
    if node_id:
        rows = q("SELECT ts,lat,lon,alt FROM positions WHERE node_id=? AND ts>=? ORDER BY ts", (node_id, since))
    else:
        # tutte le tracce dei nodi tracciati
        rows = q("""SELECT p.node_id, p.ts, p.lat, p.lon FROM positions p
                    JOIN nodes n ON n.node_id=p.node_id AND n.tracked=1
                    WHERE p.ts>=? ORDER BY p.node_id, p.ts""", (since,))
    return jsonify(rows)

@app.route("/api/meshstats")
def meshstats():
    now = int(datetime.datetime.now().timestamp())
    h1, d1, d7 = now - 3600, now - 86400, now - 7 * 86400
    def scalar(sql, args=()):
        r = q(sql, args)
        return list(r[0].values())[0] if r else 0
    stats = {
        "nodes_total":     scalar("SELECT COUNT(*) FROM nodes"),
        "nodes_last_hour": scalar("SELECT COUNT(*) FROM nodes WHERE last_heard>=?", (h1,)),
        "nodes_last_day":  scalar("SELECT COUNT(*) FROM nodes WHERE last_heard>=?", (d1,)),
        "nodes_week":      scalar("SELECT COUNT(*) FROM nodes WHERE last_heard>=?", (d7,)),
        "nodes_with_env":  scalar("SELECT COUNT(*) FROM nodes WHERE has_env=1"),
        "nodes_with_pos":  scalar("SELECT COUNT(*) FROM nodes WHERE lat IS NOT NULL"),
        "msg_total":       scalar("SELECT COUNT(*) FROM messages"),
        "msg_today":       scalar("SELECT COUNT(*) FROM messages WHERE ts>=?", (d1,)),
        "channels_enabled":scalar("SELECT COUNT(*) FROM channels WHERE role!=0"),
        "max_hops":        scalar("SELECT COALESCE(MAX(hops),0) FROM nodes"),
        "direct_nodes":    scalar("SELECT COUNT(*) FROM nodes WHERE hops=0 AND last_heard>=?", (d1,)),
    }
    stats["top_senders"] = q("""SELECT from_name AS name, COUNT(*) AS n FROM messages
                                WHERE outgoing=0 AND from_name IS NOT NULL
                                GROUP BY from_name ORDER BY n DESC LIMIT 6""")
    stats["top_hw"] = q("""SELECT hw, COUNT(*) AS n FROM nodes WHERE hw IS NOT NULL
                           GROUP BY hw ORDER BY n DESC LIMIT 6""")
    return jsonify(stats)

@app.route("/api/records")
def records():
    now = int(datetime.datetime.now().timestamp())
    start_today = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    def rec(col, order):
        r = q(f"SELECT ts,{col} v FROM readings WHERE {col} IS NOT NULL ORDER BY {col} {order} LIMIT 1")
        return r[0] if r else None
    def today(col, fn):
        r = q(f"SELECT {fn}({col}) v FROM readings WHERE ts>=? AND {col} IS NOT NULL", (start_today,))
        return r[0]["v"] if r and r[0]["v"] is not None else None
    # trend: valore attuale vs ~1h fa
    cur = q("SELECT temperature,pressure,humidity FROM readings ORDER BY ts DESC LIMIT 1")
    past = q("SELECT temperature,pressure,humidity FROM readings WHERE ts<=? ORDER BY ts DESC LIMIT 1", (now - 3600,))
    trend = {}
    if cur and past:
        for k in ("temperature", "pressure", "humidity"):
            if cur[0][k] is not None and past[0][k] is not None:
                trend[k] = round(cur[0][k] - past[0][k], 2)
    pres_max = rec("pressure", "DESC"); pres_min = rec("pressure", "ASC")
    if PRESSURE_OFFSET:
        for r in (pres_max, pres_min):
            if r and r.get("v") is not None:
                r["v"] = padj(r["v"])
    return jsonify({
        "temp_max": rec("temperature", "DESC"), "temp_min": rec("temperature", "ASC"),
        "hum_max":  rec("humidity", "DESC"),     "hum_min":  rec("humidity", "ASC"),
        "pres_max": pres_max,                    "pres_min": pres_min,
        "today": {
            "temp_min": today("temperature", "MIN"), "temp_max": today("temperature", "MAX"),
            "hum_min":  today("humidity", "MIN"),    "hum_max":  today("humidity", "MAX"),
            "pres_min": padj(today("pressure", "MIN")), "pres_max": padj(today("pressure", "MAX")),
        },
        "trend_1h": trend,
    })

@app.route("/api/send", methods=["POST", "OPTIONS"])
def send():
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    channel = data.get("channel", 0)
    if not text:
        return jsonify({"error": "testo vuoto"}), 400
    if len(text.encode("utf-8")) > 220:
        return jsonify({"error": "messaggio troppo lungo (max ~220 byte)"}), 400
    try:
        channel = int(channel)
    except (TypeError, ValueError):
        return jsonify({"error": "canale non valido"}), 400
    enabled = [r["idx"] for r in q("SELECT idx FROM channels WHERE role!=0")]
    if enabled and channel not in enabled:
        return jsonify({"error": "canale non abilitato"}), 400
    reply_id = data.get("reply_id")
    try:
        reply_id = int(reply_id) if reply_id else None
    except (TypeError, ValueError):
        reply_id = None
    con = sqlite3.connect(DB)
    con.execute("INSERT INTO outbox(ts,channel,text,status,reply_id) VALUES(?,?,?,'pending',?)",
                (int(datetime.datetime.now().timestamp()), channel, text, reply_id))
    con.commit(); con.close()
    return jsonify({"queued": True})

@app.route("/api/stats")
def stats():
    period = request.args.get("period", "day")
    fmt   = {"day": "%Y-%m-%d", "month": "%Y-%m", "year": "%Y"}.get(period, "%Y-%m-%d")
    limit = {"day": 62, "month": 24, "year": 20}.get(period, 62)
    con = sqlite3.connect(DB)
    con.create_function("dewf", 2, dew_point)
    con.row_factory = sqlite3.Row
    sql = f"""
      SELECT strftime('{fmt}', ts, 'unixepoch', 'localtime') AS period,
        COUNT(*) AS n,
        AVG(temperature) t_avg, MIN(temperature) t_min, MAX(temperature) t_max,
        AVG(humidity)    h_avg, MIN(humidity)    h_min, MAX(humidity)    h_max,
        AVG(pressure)    p_avg, MIN(pressure)    p_min, MAX(pressure)    p_max,
        AVG(dewf(temperature,humidity)) d_avg,
        MIN(dewf(temperature,humidity)) d_min,
        MAX(dewf(temperature,humidity)) d_max
      FROM readings
      GROUP BY period ORDER BY period DESC LIMIT ?
    """
    try:
        rows = [dict(r) for r in con.execute(sql, (limit,))]
    finally:
        con.close()
    if PRESSURE_OFFSET:
        for r in rows:
            for k in ("p_avg", "p_min", "p_max"):
                r[k] = padj(r[k])
    return jsonify(rows)

# ---------------- MeshCore: azioni di rete + automazioni ----------------

def _ensure_mc(con):
    try:
        con.execute("PRAGMA busy_timeout=8000")
    except Exception:
        pass
    con.execute("""CREATE TABLE IF NOT EXISTS mc_commands(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, action TEXT, params TEXT,
        status TEXT DEFAULT 'pending', result TEXT, error TEXT, done_ts INTEGER)""")
    con.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")

def _enqueue(action, params):
    con = sqlite3.connect(DB); _ensure_mc(con)
    cur = con.execute("INSERT INTO mc_commands(ts,action,params,status) VALUES(?,?,?,'pending')",
                      (int(datetime.datetime.now().timestamp()), action, json.dumps(params)))
    cid = cur.lastrowid
    con.commit(); con.close()
    return cid

@app.route("/api/mc/advert", methods=["POST", "OPTIONS"])
def mc_advert():
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.get_json(silent=True) or {}
    cid = _enqueue("advert", {"flood": bool(data.get("flood"))})
    return jsonify({"queued": True, "id": cid})

@app.route("/api/mc/ping", methods=["POST", "OPTIONS"])
def mc_ping():
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.get_json(silent=True) or {}
    nid = data.get("node_id")
    if not nid:
        return jsonify({"error": "node_id mancante"}), 400
    cid = _enqueue("ping", {"node_id": nid})
    return jsonify({"queued": True, "id": cid})

@app.route("/api/mc/commands")
def mc_commands_list():
    limit = request.args.get("limit", default=20, type=int)
    limit = max(1, min(limit, 100))
    try:
        rows = q("SELECT id,ts,action,params,status,result,error,done_ts FROM mc_commands ORDER BY id DESC LIMIT ?", (limit,))
    except Exception:
        rows = []
    return jsonify(rows)

@app.route("/api/mc/config", methods=["GET", "POST", "OPTIONS"])
def mc_config():
    if request.method == "OPTIONS":
        return ("", 204)
    con = sqlite3.connect(DB); _ensure_mc(con)
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        try:
            v = str(max(0, int(data.get("auto_advert_min", 0))))
        except (TypeError, ValueError):
            v = "0"
        con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('auto_advert_min',?)", (v,))
        con.commit()
    row = con.execute("SELECT value FROM meta WHERE key='auto_advert_min'").fetchone()
    con.close()
    return jsonify({"auto_advert_min": int(row[0]) if row and row[0] else 0})

@app.route("/static/<path:p>")
def static_files(p):
    return send_from_directory(app.static_folder, p)

if __name__ == "__main__":
    from waitress import serve
    print(f"Dashboard su http://0.0.0.0:{PORT}  (DB={DB})", flush=True)
    serve(app, host="0.0.0.0", port=PORT, threads=4)
