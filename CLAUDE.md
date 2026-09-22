# Meshlogger — Raspberry Pi (stazione ambientale + monitor mesh MeshCore)

Progetto su Raspberry Pi che logga **temperatura/umidità/pressione** da un nodo LoRa,
mostra grafici/statistiche su una **dashboard web**, gestisce la **chat** dei canali
mesh (ricezione/invio) e una **mappa** dei nodi. Originariamente su **Meshtastic**, ora
convertito a **MeshCore** (dal 2026-08-24).

> Documento di contesto per riprendere il lavoro in futuro. Il codice gira **sul Pi**
> in `/home/pi/meshlogger/`; questo repo ne è la copia versionata (deploy via `scp`).
> **I valori sensibili (chiavi, IP, dominio) sono placeholder `<...>`**: i valori reali
> stanno in `SECRETS.local.md` (in `.gitignore`, non versionato).

---

## 1. Accesso e rete

| Cosa | Valore |
|---|---|
| Raspberry Pi | `pi.local` (IP DHCP variabile), utente `pi` |
| SSH (dal PC Windows) | `ssh -i ~/.ssh/<chiave> pi@pi.local` (chiave, senza password) |
| Chiave SSH (Windows) | `<percorso-chiave-ssh>` (iniettata via cloud-init sulla SD perché la password era persa) |
| Modello / OS | Raspberry Pi 3B · Debian 13 (trixie) · Python 3.13 |
| Nodo MeshCore | IP DHCP variabile, **porta companion TCP 5000**. `MC_HOST=auto` → il logger lo **scopre** scandendo la LAN (fallback anche con IP fisso non raggiungibile) |
| Nodo/sensore | nodo "MALO", pubkey `<PUBKEY>`; BME280 collegato → telemetria su **canale LPP 2** |
| Dashboard LAN | http://<IP_PI>:8080 (o pi.local:8080) |
| Dashboard/API remota | https://<TUO_DOMINIO>.ngrok-free.dev (ngrok) |
| Chiave API | `<API_KEY>` (in `SECRETS.local.md`) |

---

## 2. Servizi systemd (`/etc/systemd/system/`)

- **`meshcorelogger.service`** — logger MeshCore attivo (`logger_meshcore.py`).
  Env: `MC_CONN=tcp MC_HOST=auto MC_TCP_PORT=5000 MC_SENSOR=self MESH_INTERVAL=60`
  (`MC_HOST=auto` → il nodo viene scoperto sulla LAN; vedi §7 robustezza IP).
- **`meshweb.service`** — backend Flask + dashboard su :8080 (`web.py`).
  Env: `WEB_PORT=8080 API_KEY=… PRESSURE_OFFSET=11 MESH_NODE_LABEL=…`.
- **`meshngrok.service`** — tunnel ngrok verso il dominio fisso.
- **`meshlogger.service`** — vecchio logger **Meshtastic**, **fermo** (riavviabile se si torna a Meshtastic).

Tutti (tranne meshlogger) `enabled` → ripartono al boot. Token ngrok in `~/.config/ngrok/ngrok.yml`.

---

## 3. File in `/home/pi/meshlogger/`

| File | Ruolo |
|---|---|
| `logger_meshcore.py` | **Daemon MeshCore attuale** (asyncio): telemetria + chat canali + nodi/mappa + canali |
| `logger.py` | Vecchio daemon Meshtastic (non in uso) |
| `web.py` | Backend Flask + API (waitress su :8080) — **invariato tra Meshtastic e MeshCore** |
| `templates/index.html` | Dashboard completa (HTML/CSS/JS inline) — **invariata** |
| `static/` | Librerie locali: chart.umd.min.js, chartjs-plugin-zoom.min.js, hammer.min.js, leaflet.js/.css |
| `meshlogger.db` | SQLite (dati) — NON versionare |
| `venv/` | virtualenv: `meshtastic`, `meshcore`, flask, waitress — NON versionare |
| `PROJECT.md` | doc storica (era Meshtastic) |

---

## 4. Schema database (SQLite `meshlogger.db`)

- `readings(ts PK, iso, temperature, humidity, pressure, metrics)` — 1 riga/minuto.
- `messages(id PK, ts, iso, from_id, from_name, to_id, channel, msg_id, text, outgoing, reply_id, path, path_len, snr, UNIQUE(from_id,msg_id,text))` — `path`=hex hash dei salti, `path_len`=n. salti, `snr` dB.
  **`channel = -1` (`DM_CHANNEL`) = messaggio diretto**: `from_id` = nodo mittente (in arrivo), `to_id` = nodo destinatario (in uscita).
- `outbox(id PK, ts, channel, text, status, error, reply_id, to_id)` — coda invio; con `to_id` valorizzato è un DM (`send_msg`), altrimenti messaggio di canale.
- `channels(idx PK, name, role)` — role 1=primario, 2=secondario, 0=disabilitato.
- `nodes(node_id PK, num, long_name, short_name, hw, role, last_heard, snr, hops, battery, voltage, has_env, lat, lon, updated, tracked)`
- `positions(id PK, node_id, ts, lat, lon, alt)` — storico posizioni tracciati, retention 31gg.
- `meta(key PK, value)`

---

## 5. API (tutte sotto `/api`, chiave via header `X-API-Key` o `?key=`; CORS aperto)

| Endpoint | Descrizione |
|---|---|
| `GET /api/latest` | ultima lettura + conteggi |
| `GET /api/readings?hours=N` | serie temporale (con dew point + medie mobili `t_ma`/`p_ma` centrate 10min) |
| `GET /api/stats?period=day\|month\|year` | media/min/max aggregati |
| `GET /api/records` | record assoluti/oggi + trend 1h |
| `GET /api/messages?limit=N&channel=C` | messaggi (filtrabili per canale; `channel=-1` = DM, `peer=<node_id>` = conversazione diretta con un nodo) |
| `POST /api/send` | invia `{text, channel, reply_id?}` → coda outbox |
| `POST /api/messages/clear` | svuota una conversazione: `{channel}` (canale) o `{peer}` (DM con un nodo) → `{deleted}` |
| `GET /api/dmpeers` | nodi con cui esiste una conversazione diretta (id, nome, ultimo ts, n. messaggi) |
| `POST /api/dm` | invia `{node_id, text}` → coda outbox come messaggio diretto |
| `GET /api/channels` | canali abilitati |
| `GET /api/meshstats` | conteggi nodi/attivi/messaggi, top_senders, top_hw |
| `GET /api/nodes?pos=1&tracked=1&limit=N` | elenco nodi |
| `GET /api/node?id=<id>` | dettaglio nodo + ultima posizione |
| `POST /api/track` | `{node_id, tracked}` traccia/non traccia |
| `GET /api/positions?node_id=<id>&days=30` | storico posizioni |
| `POST /api/mc/advert` | `{flood}` → accoda un advertise |
| `POST /api/mc/ping` | `{node_id}` → accoda un ping (path discovery) al nodo |
| `GET /api/mc/commands?limit=N` | esito comandi MeshCore (coda) |
| `POST /api/mc/prune` | `{days}` → accoda la pulizia della rubrica del nodo (rimuove i contatti non visti da N giorni) |
| `GET\|POST /api/mc/config` | legge/imposta `{auto_advert_min, auto_prune_days}`; in lettura riporta anche `contacts_count`/`max_contacts` |

Per accesso via ngrok aggiungere header `ngrok-skip-browser-warning: true` (evita l'interstitial free).

---

## 6. Dashboard — funzionalità (tab)

- **📈 Grafici**: stat tile (temp/umidità/pressione/dew point), striscia record, 4 grafici con
  **zoom/pan**, **medie mobili centrate 10 min**, tabelle statistiche giorno/mese/anno.
  Pressione con **offset +11 hPa** applicato in serving (dati grezzi intatti).
- **💬 Messaggi**: la **chat è il primo blocco della pagina** (statistiche mesh sotto). Chat
  **per canale** (sotto-tab Public/Italia/Veneto) più la sotto-tab **✉ Diretti** con una
  conversazione per nodo (chip dei nodi sopra la chat); ordine cronologico (recenti in basso),
  input sotto la chat, clic sul nome mittente → **popup info nodo**. Tasto **🗑 Svuota chat**:
  cancella dal DB i messaggi della sola conversazione aperta (canale o DM), previa conferma.
- **🗺️ Mappa** (Leaflet + OSM): marker nodi, **filtri Tracciati/Con posizione/Tutti** (default
  "Tracciati"), **slider età** (1h→30gg), ricerca, flag **traccia**, percorsi, **auto-refresh 30s**.
- **🛰️ Rete** (funzioni MeshCore): **Advertise** (manuale, con opz. flood) + **advertise automatico
  ogni N minuti**; **occupazione rubrica** (`usati / max_contacts`) con **pulizia** dei contatti non
  visti da N giorni (manuale o automatica ogni ora); tabella **nodi** con badge tipo
  (Client/Ripetitore/Room/Sensore), filtro per tipo, ricerca, **Ping** per riga (path discovery →
  raggiungibile+tempo o "nessuna risposta") e **✉** per aprire un messaggio diretto; log azioni.
- Tema chiaro/scuro; chiave API salvata in localStorage (tasto 🔑).

---

## 7. MeshCore — specifiche e quirk IMPORTANTI

- Libreria Python **`meshcore`** (PyPI), **asyncio + eventi** (diversa da Meshtastic a callback).
  Connessioni: `MeshCore.create_tcp(host, 5000)` / `create_serial(port)` / `create_ble(...)`.
- **Telemetria**: `commands.get_self_telemetry()` (nodo locale) → evento `TELEMETRY_RESPONSE`
  in **LPP Cayenne**. Tipi: 103=Temperatura(/10), 104=Umidità(/2), 115=Barometro(/10, pressione hPa).
  Il **BME è sul canale LPP 2** (ch1=scheda: voltage+temp interna ~39°C DA IGNORARE). La risposta è
  **intermittente** → il logger ritenta fino a 4 volte finché arriva il canale con umidità/pressione.
- **`is_error` è un METODO** dell'Event (va chiamato: `ev.is_error()`), non un attributo.
- **Canali**: `commands.get_channel(idx)` → `{channel_idx, channel_name, channel_secret, channel_hash}`.
  Attivo = nome non vuoto o secret non tutto-zero. Su questo nodo: CH0 Public, CH1 Italia, CH2 Veneto
  (aggiunti con `commands.set_channel(idx, name, bytes.fromhex(key))`).
  Chiavi: Italia `<CHIAVE_ITALIA>`, Veneto `<CHIAVE_VENETO>` (valori in `SECRETS.local.md`).
- **Chat canali**: ricezione via `subscribe(EventType.CHANNEL_MSG_RECV, cb)` + `start_auto_message_fetching()`.
  Il **nome mittente è DENTRO il testo** (`"Nome: testo"`) → funzione `_split_name`. Niente reply/msg_id
  crittografico (uso `sender_timestamp` come pseudo-id per dedup). Invio via `commands.send_chan_msg(chan, testo)`.
- **Nodi = contatti**: `commands.get_contacts()` / `mc.contacts` (dict). Campi: `public_key`, `adv_name`,
  `adv_lat/adv_lon`, `last_advert`, `type` (1=Chat,2=Repeater,3=Room,4=Sensor), `out_path_len`.
  `node_id = public_key[:12]`. init_db ripulisce i vecchi nodi Meshtastic (`node_id LIKE '!%'`).
- **Percorso messaggi (route)**: i messaggi di canale mostrano n. salti + sequenza nodi + SNR. Il path
  completo NON è nell'evento `CHANNEL_MSG_RECV` (solo `path_len`); arriva dagli **RX log** (`RX_LOG_DATA`,
  `payload_typename='GRP_TXT'`, campo `path` = hex hash dei salti, `path_hash_size` 1-2 byte). Cattura:
  (a) `mc.set_decrypt_channel_logs(True)` fa correlare path/SNR dalla libreria (reader.py, via hash testo);
  (b) FALLBACK robusto: bufferizzo gli RX log GRP_TXT (`on_rx_log`) e li correlo per tempo in `on_chan_msg`
  (`_match_recent_log`). web `_resolve_paths` mappa gli hash→nome nodo per prefisso pubkey (node_id[:2]/[:4]).
  NB: verificabile solo con traffico reale di canale (mesh spesso silenziosa); il log MSG riporta `[salti=.. percorso=sì/no snr=..]`.
- **DM (messaggi diretti)**: ricezione via `subscribe(EventType.CONTACT_MSG_RECV, on_contact_msg)`;
  il payload ha `pubkey_prefix` (6 byte hex = **esattamente il `node_id`**), `text` (senza prefisso
  "Nome: ", diverso dai canali), `sender_timestamp` (usato come `msg_id` per il dedup) e
  `path_len` (**255 = consegna diretta** → lo normalizzo a 0). Invio: riga in `outbox` con `to_id`
  → `mc.commands.send_msg(contact, testo)`; in `messages` i DM stanno su `channel=-1`.
- **Rubrica del nodo piena (limite firmware)**: `send_device_query()` riporta `max_contacts`
  (su questo Heltec V4: **350**). **Quando la rubrica è piena il firmware NON registra più nodi
  nuovi** — sintomo: il numero di nodi resta fisso e i nodi nuovi non compaiono mai. Rimedio:
  `prune_contacts()` rimuove dal nodo i contatti con `last_advert` più vecchio di N giorni
  (`remove_contact(public_key)`, servono i 32 byte interi). I nodi `tracked` non si toccano e la
  tabella `nodes` (storico dashboard) non viene svuotata.
- **La cache dei contatti della libreria non si svuota mai**: `get_contacts()` fa solo *merge* in
  `mc.contacts`, quindi dopo una `remove_contact` il conteggio resta quello vecchio finché non si
  riconnette. Per questo `prune_contacts` fa `mc.contacts.pop(pk)` a mano.
- **Non ancora portato da MeshCore**: batteria/SNR/has_env per singolo nodo (servirebbero
  `req_status`/`req_telemetry` per contatto).

### Azioni di rete e AUTOMAZIONI (architettura estensibile)
Il web non ha la connessione al nodo (un solo client TCP, tenuto dal logger) → le azioni passano da
una **coda comandi** `mc_commands(id,ts,action,params,status,result,error,done_ts)`:
1. il web accoda con `_enqueue(action, params)` (endpoint `/api/mc/*`);
2. il logger `process_mc_commands()` (ogni ciclo) esegue l'azione sulla connessione e riscrive `result`.
Azioni attuali: `advert` (`send_advert(flood)`), `prune` (pulizia rubrica), `ping` **type-aware** — il contatto si ottiene con
`mc.get_contact_by_key_prefix(node_id)`:
- **companion (type 1)**: NON è un vero ping → invia un messaggio `"ping"` (`send_msg`) e attende l'**ACK
  di consegna** (subscribe `EventType.ACK`; l'ACK ha `code` = `expected_ack.hex()` del MSG_SENT, e `trip_time` ms).
  Il frontend mostra un **popup di conferma** che avvisa che verrà inviato un messaggio.
- **ripetitore/room (type 2/3)**: `send_path_discovery_sync(contact, 25s)` (silenzioso).
Fatto empirico: i companion NON rispondono al path discovery (KDNZ, in portata, → nessuna risposta);
rispondono invece all'ACK del DM (KDNZ ~0.9s). SQLite in **WAL + busy_timeout=8000** per evitare lock.
**Automazioni periodiche**: chiavi in `meta` lette con `get_meta_int()` + timer nel loop —
`auto_advert_min` (advertise) e `auto_prune_days` (pulizia rubrica, al più una volta l'ora). **Per aggiungere una nuova automazione periodica**: (a) nuova chiave in `meta`,
(b) nuovo ramo in `process_mc_commands` (se on-demand) o un timer nel loop (se periodico), (c) endpoint
`/api/mc/...`, (d) UI nella tab Rete. Esempi facili: telemetria periodica a un nodo, path-discovery
schedulato, richiesta stato ai ripetitori.

### Robustezza connessione (anti-blocco)
Sintomo tipico: servizio `active` ma **offline** (nessun `SAVED`), senza errori nei log.
Causa: **TCP half-open** — il nodo riavvia/perde il WiFi senza chiudere il socket, `is_connected`
resta `True`, l'`await` sulla telemetria si appende all'infinito. Tre difese in `logger_meshcore.py`:
1. **Timeout** su connessione (`MC_CONNECT_TIMEOUT=30`) e su ogni richiesta al nodo (`MC_CALL_TIMEOUT=25`)
   via `asyncio.wait_for` → un await appeso alza `TimeoutError` e innesca la riconnessione.
2. **Riconnessione per staleness** (`MC_STALE_RECONNECT`, default `max(240, INTERVAL*4)`s): se "connesso"
   ma senza dati salvati da troppo tempo, `disconnect()` + `mc=None` → riconnette (ri-scoprendo l'IP).
3. **Watchdog di processo** (`MC_STALL_LIMIT`, default `max(300, INTERVAL*5)`s): task separato; se il loop
   non fa progressi (`_progress()`), `os._exit(1)` → systemd riavvia. NB: mentre il nodo è spento il loop
   RITENTA (è progresso) → il watchdog NON scatta. systemd: `Restart=always`, `StartLimitIntervalSec=0`.

### Quirk generali (validi anche in Meshtastic)
- Il nodo accetta **UN SOLO client TCP** per volta → un unico daemon persistente. Per interrogare il
  nodo con uno script di test bisogna **fermare `meshcorelogger`** prima.
- **Jinja cache i template** → dopo aver modificato `index.html` serve `sudo systemctl restart meshweb`.
- Medie mobili **centrate** (±window/2), non trailing (una trailing "sembra sbagliata" perché in ritardo).
- **Letture fuori scala**: i guasti elettrici producono valori assurdi (T=179, P=-142). Il logger
  scarta la lettura se un valore esce dai range fisici del BME280 (`LIMITS`: T −40..85 °C,
  RH 0..100 %, P 300..1100 hPa) — NON si usa il range climatico locale, o d'inverno si perderebbero
  letture valide.

---

## 8. Workflow di modifica/deploy

1. Modifica i file in locale (scratchpad), poi:
   `scp -i ~/.ssh/<chiave> <file> pi@pi.local:/home/pi/meshlogger/…`
2. Riavvia il servizio giusto:
   - `web.py` o `templates/index.html` → `sudo systemctl restart meshweb`
   - `logger_meshcore.py` → `sudo systemctl restart meshcorelogger`
3. Verifica: `systemctl is-active meshcorelogger meshweb meshngrok` e `journalctl -u <servizio> -n 20`.

Dipendenze venv: `pip install meshcore meshtastic flask waitress`.
Librerie static/ da jsdelivr: chart.js 4.4.4, chartjs-plugin-zoom 2.0.1, hammerjs 2.0.8, leaflet 1.9.4.

---

## 9. Segreti — NON versionare

Chiave API (in `meshweb.service`) · token ngrok (`~/.config/ngrok/ngrok.yml`) · chiave SSH
(sul PC) · `meshlogger.db` · `venv/` · `SECRETS.local.md`.

---

## 10. Stato

**Fatto (su MeshCore):** telemetria (temp/umidità/pressione), canali (Public/Italia/Veneto),
chat canali (ricezione+invio), **DM ai contatti** (ricezione+invio, tab ✉ Diretti e tasto ✉ nella
tab Rete), mappa/nodi, **gestione della rubrica del nodo** (occupazione + pulizia manuale/automatica).

**Manutenzione fatta il 2026-09-22:** eliminate 247 letture corrotte da un guasto elettrico
(226 con T fuori 10–50 °C + 21 con pressione fuori 900–1100 hPa; backup `meshlogger.db.bak-2026-09-22`
sul Pi) e liberata la rubrica del nodo, piena a 350/350: rimossi 153 contatti inattivi da oltre
14 giorni → 197, così i nodi nuovi tornano a registrarsi.

**Possibili prossimi passi:** batteria/SNR/env per nodo sulla mappa; notifica/badge sui DM non letti;
attivare la pulizia rubrica automatica (`auto_prune_days`) per non tornare a rubrica piena.
