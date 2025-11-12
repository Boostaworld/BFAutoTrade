import json
import asyncio
import random
import re
from pathlib import Path
from datetime import datetime
import discord

@nightyScript(
    name="Blox Fruits TraderV2",
    author="Grok",
    description="Auto-send trades to Blox Fruits channels",
    version="3.4"
)
def blox_fruits_trader():
    BASE_DIR = Path(getScriptsPath()) / "json"
    DATA_FILE = BASE_DIR / "blox_trader.json"
    EMOJI_CACHE_FILE = BASE_DIR / "guild_emojis.json"
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    DEFAULT_DATA = {
        "trade_channels": [],
        "trade_offers": [],
        "trade_requests": []
    }

    FRUIT_ALIASES = {
        "leopard": ["tiger"], "rumble": ["lightning"], "spirit": ["soul"],
        "t-rex": ["trex", "rex"], "control": ["kage"], "dough": ["doughnut"],
        "buddha": ["budha"], "phoenix": ["phenix", "pheonix"]
    }

    TOKEN_PATTERN = re.compile(r'<a?:[^:]+:\d+>|:[^:\s]+:|[^,\s]+')
    LITERAL_EMOJI_PATTERN = re.compile(r'^<a?:[^:]+:\d+>$')
    COLON_EMOJI_PATTERN = re.compile(r'^:[^:\s]+:$')
    COMPOUND_SPLIT_PATTERN = re.compile(r'([~])')
    SEPARATOR_TOKENS = {"~"}

    def parse_trade_input(raw):
        if not raw:
            return []
        return TOKEN_PATTERN.findall(raw)

    def normalize_trade_entries(values):
        if not isinstance(values, list):
            return []

        normalized = []
        for value in values:
            if isinstance(value, str):
                normalized.extend(parse_trade_input(value))
            elif value is not None:
                normalized.append(str(value))

        return [v for v in normalized if v]

    def looks_like_literal_emoji(token):
        return bool(LITERAL_EMOJI_PATTERN.match(token) or COLON_EMOJI_PATTERN.match(token))

    class AutoState:
        running = False
        batch_running = False
        task = None
        should_stop = False

    def load_data():
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                for k, v in DEFAULT_DATA.items():
                    if k not in data:
                        data[k] = v

                normalized_offers = normalize_trade_entries(data.get("trade_offers", []))
                normalized_requests = normalize_trade_entries(data.get("trade_requests", []))

                changed = False
                if normalized_offers != data.get("trade_offers"):
                    data["trade_offers"] = normalized_offers
                    changed = True
                if normalized_requests != data.get("trade_requests"):
                    data["trade_requests"] = normalized_requests
                    changed = True

                if changed:
                    save_data(data)
                return data
        except:
            return DEFAULT_DATA.copy()

    def save_data(data):
        try:
            tmp = DATA_FILE.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(data, f, indent=4)
            tmp.replace(DATA_FILE)
        except:
            pass

    def load_emoji_cache():
        try:
            with open(EMOJI_CACHE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}

    def save_emoji_cache(cache):
        try:
            with open(EMOJI_CACHE_FILE, "w") as f:
                json.dump(cache, f, indent=4)
        except:
            pass

    def get_cooldown_remaining(last_sent, cooldown):
        if not last_sent:
            return 0
        try:
            last = datetime.fromisoformat(last_sent)
            elapsed = (datetime.now() - last).total_seconds()
            return max(0, int(cooldown - elapsed))
        except:
            return 0

    data = load_data()
    emoji_cache = load_emoji_cache()

    # UI
    tab = Tab(name='BF Trader', title="Blox Fruits Trader", icon="convert")
    main = tab.create_container(type="rows")
    card = main.create_card(height="full", width="full", gap=3)

    # Inputs
    top = card.create_group(type="columns", gap=3, full_width=True)
    srv_in = top.create_ui_element(UI.Input, label="Server ID", full_width=True, show_clear_button=True)
    ch_in = top.create_ui_element(UI.Input, label="Channel IDs", full_width=True, show_clear_button=True)
    cd_in = top.create_ui_element(UI.Input, label="Cooldown", value="60", full_width=True)
    add_btn = top.create_ui_element(UI.Button, label='Add', disabled=True, color="default")
    det_btn = top.create_ui_element(UI.Button, label='Detect', color="default")

    # Trade
    trade = card.create_group(type="columns", gap=3, full_width=True)
    off_in = trade.create_ui_element(UI.Input, label="Offering", placeholder="dough, spirit, OR, trex", full_width=True, show_clear_button=True)
    req_in = trade.create_ui_element(UI.Input, label="Requesting", placeholder="rumble, tiger", full_width=True, show_clear_button=True)
    save_btn = trade.create_ui_element(UI.Button, label='Save', disabled=True, color="default")

    # Controls
    ctrl = card.create_group(type="columns", gap=3, full_width=True)
    auto_check = ctrl.create_ui_element(UI.Checkbox, label='Auto Send Mode', checked=False)
    start_btn = ctrl.create_ui_element(UI.Button, label='Start', disabled=True, color="success")
    stop_btn = ctrl.create_ui_element(UI.Button, label='Stop', disabled=True, color="danger")

    # Tables
    tables = card.create_group(type="columns", gap=6, full_width=True)
    
    ch_table = None
    tr_table = tables.create_ui_element(
        UI.Table, selectable=False, search=False, items_per_page=5,
        columns=[{"type": "text", "label": "Trade"}], rows=[]
    )

    # Helper Functions
    async def find_trade_emoji(guild):
        try:
            # First priority: search for "trade" at the START of emoji name
            for e in guild.emojis:
                name_lower = e.name.lower()
                if name_lower.startswith("trade") or "_trade" in name_lower or name_lower == "trade" or name_lower.endswith("_trade"):
                    return f"<:{e.name}:{e.id}>"

            # Default to arrows emoji
            return "↔️"
        except:
            return "↔️"

    async def find_or_emoji(guild):
        try:
            for e in guild.emojis:
                n = e.name.lower()
                if n in ["or"] or n.startswith("or_") or n.endswith("_or"):
                    return f"<:{e.name}:{e.id}>" if not e.animated else f"<a:{e.name}:{e.id}>"
            return "🔁"
        except:
            return "🔁"

    async def resolve_compound_token(gid, term):
        parts = COMPOUND_SPLIT_PATTERN.split(term)
        resolved = []
        any_found = False

        for part in parts:
            if not part:
                continue
            if COMPOUND_SPLIT_PATTERN.fullmatch(part):
                resolved.append(part)
                continue

            sub = await fetch_emoji(gid, part)
            if sub:
                resolved.append(sub)
                any_found = True
            else:
                resolved.append(part)

        if any_found:
            return "".join(resolved)
        return None

    async def fetch_emoji(gid, term):
        term = term.strip()
        if not term:
            return None

        if looks_like_literal_emoji(term):
            return term

        if COMPOUND_SPLIT_PATTERN.search(term):
            combined = await resolve_compound_token(gid, term)
            if combined:
                return combined

        if term.lower() == "or":
            g = bot.get_guild(int(gid))
            return await find_or_emoji(g) if g else "🔁"

        gs = str(gid)
        tl = term.lower()

        if gs in emoji_cache and tl in emoji_cache[gs]:
            return emoji_cache[gs][tl]

        try:
            g = bot.get_guild(int(gid))
            if not g:
                return None

            for e in g.emojis:
                if tl in e.name.lower():
                    es = f"<:{e.name}:{e.id}>" if not e.animated else f"<a:{e.name}:{e.id}>"
                    if gs not in emoji_cache:
                        emoji_cache[gs] = {}
                    emoji_cache[gs][tl] = es
                    save_emoji_cache(emoji_cache)
                    return es

            if tl in FRUIT_ALIASES:
                for alias in FRUIT_ALIASES[tl]:
                    for e in g.emojis:
                        if alias in e.name.lower():
                            es = f"<:{e.name}:{e.id}>" if not e.animated else f"<a:{e.name}:{e.id}>"
                            if gs not in emoji_cache:
                                emoji_cache[gs] = {}
                            emoji_cache[gs][tl] = es
                            save_emoji_cache(emoji_cache)
                            return es
            return None
        except:
            return None

    async def build_msg(gid, offers, requests, te=None):
        g = bot.get_guild(int(gid))
        if not te:
            te = await find_trade_emoji(g) if g else "↔️"

        oe = []
        for o in offers:
            if o in SEPARATOR_TOKENS:
                oe.append(o)
                continue

            e = await fetch_emoji(gid, o.strip())
            oe.append(e if e else f"`{o.strip()}`")

        re = []
        for r in requests:
            if r in SEPARATOR_TOKENS:
                re.append(r)
                continue

            e = await fetch_emoji(gid, r.strip())
            re.append(e if e else f"`{r.strip()}`")

        return f"{' '.join(oe)} {te} {' '.join(re)}"

    async def send_to(cid, msg):
        try:
            ch = bot.get_channel(int(cid))
            if not ch:
                return False, "Not found"
            await ch.send(msg)
            return True, "OK"
        except discord.errors.Forbidden:
            return False, "No perm"
        except:
            return False, "Error"

    # Main Functions
    def sendNowToChannel_sync(row_id):
        bot.loop.create_task(sendNowToChannel(row_id))
    
    async def sendNowToChannel(cid):
        try:
            d = load_data()
            
            if not d["trade_offers"] or not d["trade_requests"]:
                print("Configure trade first", type_="WARNING")
                return
            
            channel = None
            for tc in d["trade_channels"]:
                if tc["id"] == cid:
                    channel = tc
                    break
            
            if not channel:
                print("Channel not found", type_="ERROR")
                return
            
            msg = await build_msg(channel["server_id"], d["trade_offers"], d["trade_requests"], channel.get("trade_emoji"))
            ok, err = await send_to(channel["id"], msg)
            
            if ok:
                channel["last_sent"] = datetime.now().isoformat()
                save_data(d)
                print(f"✓ Sent to {channel['channel_name']}", type_="SUCCESS")
                
                rem = get_cooldown_remaining(channel.get("last_sent"), channel.get("cooldown", 60))
                st = f"CD: {rem}s" if rem > 0 else "Ready"
                
                ch_table.update_rows([{
                    "id": cid,
                    "cells": [
                        {"text": channel.get("channel_name", "?"), "imageUrl": channel.get("server_icon", ""), "subtext": channel.get("server_name", "")},
                        {"text": f"{channel.get('cooldown', 60)}s", "subtext": st},
                        {"text": st, "subtext": channel.get("last_sent", "Never")[:19]},
                        {}
                    ]
                }])
            else:
                print(f"✗ {channel['channel_name']}: {err}", type_="ERROR")
                
        except Exception as e:
            print(f"Send error: {e}", type_="ERROR")
    
    def removeChannel_sync(row_id):
        removeChannel(row_id)
    
    def removeChannel(cid):
        try:
            d = load_data()
            d["trade_channels"] = [tc for tc in d["trade_channels"] if tc["id"] != cid]
            save_data(d)
            ch_table.delete_rows([cid])
            print(f"Removed channel {cid}", type_="SUCCESS")
        except Exception as e:
            print(f"Remove error: {e}", type_="ERROR")

    ch_table = tables.create_ui_element(
        UI.Table, selectable=False, search=True, items_per_page=10,
        columns=[
            {"type": "text", "label": "Channel"},
            {"type": "text", "label": "Cooldown"},
            {"type": "text", "label": "Status"},
            {"type": "button", "label": "Actions", "buttons": [
                {"label": "Send Now", "color": "default", "onClick": sendNowToChannel_sync},
                {"label": "Remove", "color": "danger", "onClick": removeChannel_sync}
            ]}
        ], rows=[]
    )

    async def detect():
        det_btn.loading = True
        det_btn.disabled = True
        try:
            d = load_data()
            added = 0
            kw = ["trading", "slow-trading", "fast-trading", "trade-chat", "trades", "trade"]
            ex = ["pvb", "sab"]
            
            print("Scanning servers for trading channels...", type_="INFO")
            
            for g in bot.guilds:
                for ch in g.text_channels:
                    n = ch.name.lower()
                    if any(k in n for k in kw) and not any(n.startswith(e) for e in ex):
                        cid = str(ch.id)
                        if any(tc["id"] == cid for tc in d["trade_channels"]):
                            continue
                        
                        trade_emoji = await find_trade_emoji(g)
                        
                        d["trade_channels"].append({
                            "id": cid,
                            "server_id": str(g.id),
                            "server_name": g.name,
                            "server_icon": str(g.icon.url) if g.icon else "",
                            "channel_name": ch.name,
                            "cooldown": 60,
                            "last_sent": None,
                            "trade_emoji": trade_emoji
                        })
                        
                        ch_table.insert_rows([{
                            "id": cid,
                            "cells": [
                                {"text": ch.name, "imageUrl": str(g.icon.url) if g.icon else "", "subtext": g.name},
                                {"text": "60s", "subtext": "Ready"},
                                {"text": "Ready", "subtext": "Never"},
                                {}
                            ]
                        }])
                        added += 1
                        print(f"Found: {ch.name} in {g.name}", type_="SUCCESS")
            
            save_data(d)
            print(f"✓ Detection complete: Found {added} new trading channels", type_="SUCCESS")
            
            # Enable start button if we have channels and trade configured
            if d["trade_channels"] and d["trade_offers"] and d["trade_requests"]:
                start_btn.disabled = False
                
        except Exception as e:
            print(f"Detect failed: {e}", type_="ERROR")
        finally:
            det_btn.loading = False
            det_btn.disabled = False

    async def add():
        add_btn.loading = True
        sid = srv_in.value.strip()
        cids = ch_in.value.strip()
        cd = int(cd_in.value) if cd_in.value.isdigit() else 60
        
        if not sid or not cids:
            print("Need Server ID and Channel IDs", type_="WARNING")
            add_btn.loading = False
            return
        
        try:
            g = bot.get_guild(int(sid))
            if not g:
                print("Server not found", type_="ERROR")
                add_btn.loading = False
                return
            
            d = load_data()
            for cid in [c.strip() for c in cids.split(",")]:
                ch = bot.get_channel(int(cid))
                if not ch or any(tc["id"] == cid for tc in d["trade_channels"]):
                    continue
                
                d["trade_channels"].append({
                    "id": cid,
                    "server_id": sid,
                    "server_name": g.name,
                    "server_icon": str(g.icon.url) if g.icon else "",
                    "channel_name": ch.name,
                    "cooldown": cd,
                    "last_sent": None,
                    "trade_emoji": await find_trade_emoji(g)
                })
                
                ch_table.insert_rows([{
                    "id": cid,
                    "cells": [
                        {"text": ch.name, "imageUrl": str(g.icon.url) if g.icon else "", "subtext": g.name},
                        {"text": f"{cd}s", "subtext": "Ready"},
                        {"text": "Ready", "subtext": "Never"},
                        {}
                    ]
                }])
            
            save_data(d)
            print(f"Added channels", type_="SUCCESS")
            srv_in.value = ""
            ch_in.value = ""
            add_btn.disabled = True
            
            # Enable start button if we have trade configured
            if d["trade_offers"] and d["trade_requests"]:
                start_btn.disabled = False
                
        except Exception as e:
            print(f"Add failed: {e}", type_="ERROR")
        finally:
            add_btn.loading = False

    def save_trade():
        save_btn.loading = True
        d = load_data()
        
        offers = parse_trade_input(off_in.value)
        requests = parse_trade_input(req_in.value)
        
        d["trade_offers"] = offers
        d["trade_requests"] = requests
        save_data(d)
        
        ex = [r["id"] for r in tr_table.rows]
        if ex:
            tr_table.delete_rows(ex)
        
        if offers:
            tr_table.insert_rows([{"id": "o", "cells": [{"text": f"Offering: {' '.join(offers)}"}]}])
        if requests:
            tr_table.insert_rows([{"id": "r", "cells": [{"text": f"Requesting: {' '.join(requests)}"}]}])
        
        print(f"Saved: {len(offers)} offers, {len(requests)} requests", type_="SUCCESS")
        
        save_btn.loading = False
        
        # Enable start button if we have channels
        if d["trade_channels"]:
            start_btn.disabled = False

    async def send_batch():
        AutoState.batch_running = True
        AutoState.should_stop = False
        start_btn.disabled = True
        stop_btn.disabled = False
        
        d = load_data()
        
        if not d["trade_offers"] or not d["trade_requests"]:
            print("Configure trade first", type_="WARNING")
            AutoState.batch_running = False
            start_btn.disabled = False
            stop_btn.disabled = True
            return
        
        if not d["trade_channels"]:
            print("Add channels first", type_="WARNING")
            AutoState.batch_running = False
            start_btn.disabled = False
            stop_btn.disabled = True
            return
        
        sent = skip = fail = 0
        total = len(d["trade_channels"])
        
        print(f"Starting batch send to {total} channels...", type_="INFO")
        
        for idx, c in enumerate(d["trade_channels"], 1):
            if AutoState.should_stop:
                print(f"⏸ Batch stopped at channel {idx}/{total}: {c['channel_name']}", type_="WARNING")
                break
                
            try:
                rem = get_cooldown_remaining(c.get("last_sent"), c["cooldown"])
                if rem > 0:
                    skip += 1
                    print(f"[{idx}/{total}] Skipped {c['channel_name']} (cooldown: {rem}s)", type_="INFO")
                    continue
                
                msg = await build_msg(c["server_id"], d["trade_offers"], d["trade_requests"], c.get("trade_emoji"))
                ok, err = await send_to(c["id"], msg)
                
                if ok:
                    sent += 1
                    c["last_sent"] = datetime.now().isoformat()
                    print(f"[{idx}/{total}] ✓ {c['channel_name']}", type_="SUCCESS")
                else:
                    fail += 1
                    print(f"[{idx}/{total}] ✗ {c['channel_name']}: {err}", type_="ERROR")
                
                await asyncio.sleep(random.uniform(2, 4))
            except Exception as e:
                fail += 1
                print(f"[{idx}/{total}] ✗ {c['channel_name']}: {str(e)}", type_="ERROR")
        
        save_data(d)
        
        if AutoState.should_stop:
            print(f"Batch stopped: {sent} sent, {skip} skipped, {fail} failed", type_="WARNING")
        else:
            print(f"Batch complete: {sent} sent, {skip} skipped, {fail} failed", type_="SUCCESS")
        
        AutoState.batch_running = False
        AutoState.should_stop = False
        start_btn.disabled = False
        stop_btn.disabled = True
    async def auto_loop():
        print("Auto-send loop startedV3", type_="SUCCESS")

        # Track failed channels and errors
        failed_channels = {}
        error_log = []

        while AutoState.running:
            try:
                d = load_data()

                if not d["trade_offers"] or not d["trade_requests"] or not d["trade_channels"]:
                    await asyncio.sleep(5)
                    continue

                current_time = datetime.now()
                sent_this_loop = 0

                # Sort channels by cooldown remaining (lowest first)
                channels_with_cooldowns = []
                for c in d["trade_channels"]:
                    cid = c["id"]

                    # Skip recently failed channels
                    if cid in failed_channels:
                        time_since_fail = (current_time - failed_channels[cid]).total_seconds()
                        if time_since_fail < 30:
                            continue
                        else:
                            del failed_channels[cid]

                    rem = get_cooldown_remaining(c.get("last_sent"), c["cooldown"])
                    channels_with_cooldowns.append((rem, c))

                # Sort by cooldown remaining (channels with 0 cooldown first)
                channels_with_cooldowns.sort(key=lambda x: x[0])

                # Send to channels that are ready (cooldown = 0)
                for rem, c in channels_with_cooldowns:
                    if not AutoState.running:
                        break

                    if rem <= 0:
                        cid = c["id"]
                        try:
                            msg = await build_msg(c["server_id"], d["trade_offers"], d["trade_requests"], c.get("trade_emoji"))
                            ok, err = await send_to(c["id"], msg)

                            if ok:
                                c["last_sent"] = datetime.now().isoformat()
                                sent_this_loop += 1
                                print(f"✓ Auto: {c['channel_name']}", type_="SUCCESS")
                                if cid in failed_channels:
                                    del failed_channels[cid]
                            else:
                                failed_channels[cid] = current_time
                                error_log.append(f"{c['channel_name']}: {err}")

                            await asyncio.sleep(random.uniform(2, 4))
                        except Exception as e:
                            failed_channels[cid] = current_time
                            error_log.append(f"{c['channel_name']}: {str(e)}")

                save_data(d)

                # Find the minimum wait time from all channels
                min_wait = float('inf')
                for rem, c in channels_with_cooldowns:
                    if rem < min_wait:
                        min_wait = rem

                # Wait until the next channel is ready
                if sent_this_loop > 0:
                    wait_time = 1
                elif min_wait != float('inf') and min_wait > 0:
                    wait_time = min(min_wait, 10)
                else:
                    wait_time = 5

                await asyncio.sleep(wait_time)

            except Exception as e:
                print(f"Auto-loop error: {str(e)}", type_="ERROR")
                await asyncio.sleep(10)

        # Print all errors when stopped
        if error_log:
            print(f"\n=== Auto-send Error Summary ({len(error_log)} errors) ===", type_="WARNING")
            # Group errors by type
            error_counts = {}
            for err in error_log:
                if err not in error_counts:
                    error_counts[err] = 1
                else:
                    error_counts[err] += 1

            for err, count in error_counts.items():
                print(f"  [{count}x] {err}", type_="ERROR")

        print("Auto-send loop stopped", type_="INFO")

    def start_operation():
        d = load_data()
        
        if not d["trade_offers"] or not d["trade_requests"]:
            print("Configure trade first", type_="WARNING")
            return
        
        if not d["trade_channels"]:
            print("Add channels first", type_="WARNING")
            return
        
        if auto_check.checked:
            # Start auto-send loop
            AutoState.running = True
            start_btn.disabled = True
            stop_btn.disabled = False
            AutoState.task = bot.loop.create_task(auto_loop())
        else:
            # Send one batch
            bot.loop.create_task(send_batch())

    def stop_operation():
        if auto_check.checked:
            # Stop auto-send loop
            if AutoState.running:
                AutoState.running = False
                if AutoState.task:
                    AutoState.task.cancel()
                start_btn.disabled = False
                stop_btn.disabled = True
                print("Auto-send loop stopped", type_="INFO")
        else:
            # Stop batch operation
            if AutoState.batch_running:
                AutoState.should_stop = True
                print("Stopping batch send...", type_="WARNING")

    # Event Handlers
    def on_srv_input(v):
        add_btn.disabled = not (v and ch_in.value and v.isdigit() and len(v) >= 17)
    
    def on_ch_input(v):
        add_btn.disabled = not (v and srv_in.value and srv_in.value.isdigit())
    
    def on_off_input(v):
        save_btn.disabled = not (v and req_in.value)
    
    def on_req_input(v):
        save_btn.disabled = not (v and off_in.value)
    
    srv_in.onInput = on_srv_input
    ch_in.onInput = on_ch_input
    off_in.onInput = on_off_input
    req_in.onInput = on_req_input
    
    add_btn.onClick = lambda: bot.loop.create_task(add())
    det_btn.onClick = lambda: bot.loop.create_task(detect())
    save_btn.onClick = save_trade
    start_btn.onClick = start_operation
    stop_btn.onClick = stop_operation

    # Initialization
    async def init():
        d = load_data()
        
        for c in d["trade_channels"]:
            try:
                rem = get_cooldown_remaining(c.get("last_sent"), c.get("cooldown", 60))
                st = f"CD: {rem}s" if rem > 0 else "Ready"
                
                ch_table.insert_rows([{
                    "id": c["id"],
                    "cells": [
                        {"text": c.get("channel_name", "?"), "imageUrl": c.get("server_icon", ""), "subtext": c.get("server_name", "")},
                        {"text": f"{c.get('cooldown', 60)}s", "subtext": st},
                        {"text": st, "subtext": c.get("last_sent", "Never")[:19]},
                        {}
                    ]
                }])
            except:
                pass
        
        if d.get("trade_offers") and d.get("trade_requests"):
            off_in.value = " ".join(d["trade_offers"])
            req_in.value = " ".join(d["trade_requests"])

            tr_table.insert_rows([{"id": "o", "cells": [{"text": f"Offering: {' '.join(d['trade_offers'])}"}]}])
            tr_table.insert_rows([{"id": "r", "cells": [{"text": f"Requesting: {' '.join(d['trade_requests'])}"}]}])
            
            # Enable start button if we have channels
            if d["trade_channels"]:
                start_btn.disabled = False
        
        print(f"Loaded {len(d['trade_channels'])} channels", type_="SUCCESS")

    bot.loop.create_task(init())
    tab.render()

blox_fruits_trader()
