import json
import asyncio
import random
import re
from pathlib import Path
from datetime import datetime, timedelta
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

    def make_default_data():
        return {
            "trade_channels": [],
            "trade_offers": [],
            "trade_requests": []
        }

    FRUIT_ALIASES = {
        "leopard": ["tiger"], "rumble": ["lightning"], "spirit": ["soul"],
        "t-rex": ["trex", "rex"], "control": ["kage"], "dough": ["doughnut"],
        "buddha": ["budha"], "phoenix": ["phenix", "pheonix"],
        "storage": ["capacity"], "storages": ["capacity"],
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

    def sanitize_trade_channels(raw_channels):
        if not isinstance(raw_channels, list):
            return [], bool(raw_channels)

        cleaned = []
        seen_ids = set()
        changed = False

        for entry in reversed(raw_channels):
            if not isinstance(entry, dict):
                changed = True
                continue

            cid_raw = entry.get("id") or entry.get("channel_id")
            if cid_raw is None:
                changed = True
                continue

            cid = str(cid_raw).strip()
            if not cid or cid in seen_ids:
                changed = True
                continue

            server_id = entry.get("server_id") or entry.get("guild_id")
            server_id = str(server_id).strip() if server_id else None
            if not server_id:
                changed = True
                continue

            channel_name = entry.get("channel_name") or entry.get("name") or ""
            if not isinstance(channel_name, str):
                channel_name = str(channel_name)
            channel_name = channel_name.strip()
            if not channel_name:
                changed = True
                continue

            server_name = entry.get("server_name") or entry.get("guild_name") or ""
            if not isinstance(server_name, str):
                server_name = str(server_name)

            server_icon = entry.get("server_icon") or ""
            if not isinstance(server_icon, str):
                server_icon = str(server_icon)

            try:
                cooldown = int(entry.get("cooldown", 60))
            except (TypeError, ValueError):
                cooldown = 60
            if cooldown < 0:
                cooldown = 60

            last_sent = entry.get("last_sent") if isinstance(entry.get("last_sent"), str) else None
            cooldown_until = entry.get("cooldown_until") if isinstance(entry.get("cooldown_until"), str) else None

            trade_emoji = entry.get("trade_emoji")
            if trade_emoji is not None and not isinstance(trade_emoji, str):
                trade_emoji = str(trade_emoji)

            sanitized_entry = {
                "id": cid,
                "server_id": server_id,
                "server_name": server_name,
                "server_icon": server_icon,
                "channel_name": channel_name,
                "cooldown": cooldown,
                "last_sent": last_sent,
                "trade_emoji": trade_emoji,
                "cooldown_until": cooldown_until,
            }

            base_compare = {
                "id": entry.get("id"),
                "server_id": entry.get("server_id"),
                "server_name": entry.get("server_name"),
                "server_icon": entry.get("server_icon"),
                "channel_name": entry.get("channel_name"),
                "cooldown": entry.get("cooldown"),
                "last_sent": entry.get("last_sent"),
                "trade_emoji": entry.get("trade_emoji"),
                "cooldown_until": entry.get("cooldown_until"),
            }

            if any(sanitized_entry[key] != base_compare.get(key) for key in sanitized_entry):
                changed = True

            cleaned.append(sanitized_entry)
            seen_ids.add(cid)

        cleaned.reverse()
        return cleaned, changed

    def load_data():
        load_failed = False
        try:
            with open(DATA_FILE, "r") as f:
                raw = json.load(f)
        except:
            raw = {}
            load_failed = True

        data = make_default_data()
        changed = load_failed

        sanitized_channels, channels_changed = sanitize_trade_channels(raw.get("trade_channels"))
        data["trade_channels"] = sanitized_channels
        changed = changed or channels_changed

        normalized_offers = normalize_trade_entries(raw.get("trade_offers", []))
        normalized_requests = normalize_trade_entries(raw.get("trade_requests", []))

        if normalized_offers != raw.get("trade_offers"):
            changed = True
        if normalized_requests != raw.get("trade_requests"):
            changed = True

        data["trade_offers"] = normalized_offers
        data["trade_requests"] = normalized_requests

        for tc in data.get("trade_channels", []):
            if "cooldown_until" not in tc:
                tc["cooldown_until"] = None

        if changed:
            save_data(data)

        return data

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

    def get_cooldown_remaining(channel):
        try:
            cooldown_val = int(channel.get("cooldown", 60))
        except:
            cooldown_val = 60

        base_remaining = 0
        last_sent = channel.get("last_sent")
        if last_sent:
            try:
                last = datetime.fromisoformat(last_sent)
                elapsed = (datetime.now() - last).total_seconds()
                base_remaining = max(0, int(cooldown_val - elapsed))
            except:
                base_remaining = 0

        extra_remaining = 0
        cooldown_until = channel.get("cooldown_until")
        if cooldown_until:
            try:
                target = datetime.fromisoformat(cooldown_until)
                extra_remaining = max(0, int((target - datetime.now()).total_seconds()))
            except:
                extra_remaining = 0

        return max(base_remaining, extra_remaining)

    def describe_error(err):
        if isinstance(err, str):
            return err
        try:
            return json.dumps(err)
        except TypeError:
            return str(err)

    data = load_data()
    emoji_cache = load_emoji_cache()

    # Ensure new aliases bypass any stale cache entries so they resolve immediately.
    CACHE_PURGE_KEYS = {"storage", "storages"}
    cache_purged = False
    for guild_id, entries in list(emoji_cache.items()):
        if not isinstance(entries, dict):
            emoji_cache[guild_id] = {}
            cache_purged = True
            continue

        for key in CACHE_PURGE_KEYS:
            if key in entries:
                del entries[key]
                cache_purged = True

    if cache_purged:
        save_emoji_cache(emoji_cache)

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
    test_btn = ctrl.create_ui_element(UI.Button, label='Test Format', color="default")

    # Tables
    tables = card.create_group(type="columns", gap=6, full_width=True)
    
    ch_table = None
    tr_table = tables.create_ui_element(
        UI.Table, selectable=False, search=False, items_per_page=5,
        columns=[{"type": "text", "label": "Trade"}], rows=[]
    )

    # Helper Functions
    async def find_trade_emoji_v2(guild):
        try:
            preferred_exact = {
                "point_trade", "pointtrade", "point-trade",
                "trade_point", "tradepoint"
            }
            legacy_blocklist = {"wut_offer", "wut_trade_offer"}

            for e in guild.emojis:
                n = e.name.lower()
                if n in preferred_exact:
                    return f"<a:{e.name}:{e.id}>" if e.animated else f"<:{e.name}:{e.id}>"

            for e in guild.emojis:
                n = e.name.lower()
                if n in legacy_blocklist or "wut" in n:
                    continue
                if (
                    n == "trade"
                    or n.startswith("trade")
                    or n.endswith("_trade")
                    or "_trade" in n
                    or "trade" in n
                ):
                    return f"<a:{e.name}:{e.id}>" if e.animated else f"<:{e.name}:{e.id}>"

            return "↔️"
        except:
            return "↔️"
    # Use new selection logic everywhere
    find_trade_emoji = find_trade_emoji_v2
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
        except discord.errors.HTTPException as e:
            status = getattr(e, "status", None)
            code = getattr(e, "code", None)
            retry_after = getattr(e, "retry_after", None)

            if retry_after is not None and (status == 429 or code == 20028):
                return False, {
                    "type": "cooldown",
                    "retry_after": retry_after,
                    "status": status,
                    "code": code
                }

            return False, f"HTTP error (status={status}, code={code})"
        except Exception as e:
            return False, f"Error: {e}"

    async def send_test_format():
        print("Sending test format...", type_="INFO")
        try:
            d = load_data()
            if not d["trade_offers"] or not d["trade_requests"]:
                print("Configure trade first", type_="WARNING")
                return

            server_id = None
            if d["trade_channels"]:
                server_id = d["trade_channels"][0].get("server_id")

            if not server_id:
                server_id = "0"

            msg = await build_msg(server_id, d["trade_offers"], d["trade_requests"])
            ok, err = await send_to("1390328683494903978", msg)

            if ok:
                print("✓ Test format sent to Mee6", type_="SUCCESS")
            else:
                print(f"✗ Test format failed: {describe_error(err)}", type_="ERROR")
        except Exception as e:
            print(f"Test format error: {e}", type_="ERROR")

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
                now = datetime.now()
                channel["last_sent"] = now.isoformat()
                channel["cooldown_until"] = None
                save_data(d)
                print(f"✓ Sent to {channel['channel_name']}", type_="SUCCESS")

                rem = get_cooldown_remaining(channel)
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
                if isinstance(err, dict) and err.get("type") == "cooldown":
                    try:
                        retry_seconds = float(err.get("retry_after", 0))
                    except (TypeError, ValueError):
                        retry_seconds = 0

                    if retry_seconds > 0:
                        next_time = datetime.now() + timedelta(seconds=retry_seconds)
                        channel["cooldown_until"] = next_time.isoformat()
                        save_data(d)

                        rem = get_cooldown_remaining(channel)
                        st = f"CD: {rem}s" if rem > 0 else "Ready"

                        ch_table.update_rows([
                            {
                                "id": cid,
                                "cells": [
                                    {"text": channel.get("channel_name", "?"), "imageUrl": channel.get("server_icon", ""), "subtext": channel.get("server_name", "")},
                                    {"text": f"{channel.get('cooldown', 60)}s", "subtext": st},
                                    {"text": st, "subtext": channel.get("last_sent", "Never")[:19]},
                                    {}
                                ]
                            }
                        ])

                        print(f"⌛ {channel['channel_name']}: retry in {int(retry_seconds)}s", type_="WARNING")
                        return

                try:
                    cooldown_val = int(channel.get("cooldown", 60))
                except:
                    cooldown_val = 60
                channel["cooldown_until"] = (datetime.now() + timedelta(seconds=cooldown_val)).isoformat()
                save_data(d)
                print(f"✗ {channel['channel_name']}: {describe_error(err)}", type_="ERROR")

        except Exception as e:
            print(f"Send error: {e}", type_="ERROR")
    
    def removeChannel_sync(row_id):
        removeChannel(row_id)
    
    def removeChannel(cid):
        try:
            d = load_data()
            before = len(d["trade_channels"])
            d["trade_channels"] = [tc for tc in d["trade_channels"] if tc["id"] != cid]
            removed = before - len(d["trade_channels"])

            if removed <= 0:
                print(f"Channel {cid} not found", type_="WARNING")
                return

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
            updated = 0
            kw = ["trading", "slow-trading", "fast-trading", "trade-chat", "trades", "trade"]
            ex = ["pvb", "sab"]
            
            print("Scanning servers for trading channels...", type_="INFO")
            
            for g in bot.guilds:
                for ch in g.text_channels:
                    n = ch.name.lower()
                    if any(k in n for k in kw) and not any(n.startswith(e) for e in ex):
                        cid = str(ch.id)
                        # Determine slowmode-based cooldown; default to 60 only when off/None
                        try:
                            sd = getattr(ch, 'slowmode_delay', None)
                        except Exception:
                            sd = None
                        cooldown = int(sd) if sd else 60

                        trade_emoji = await find_trade_emoji(g)

                        existing = next((tc for tc in d["trade_channels"] if tc["id"] == cid), None)
                        if existing:
                            # Refresh cooldown and metadata so auto-send respects actual timing
                            if existing.get("cooldown") != cooldown:
                                existing["cooldown"] = cooldown
                                updated += 1
                            existing["server_id"] = str(g.id)
                            existing["server_name"] = g.name
                            existing["server_icon"] = str(g.icon.url) if g.icon else ""
                            existing["channel_name"] = ch.name
                            if trade_emoji:
                                existing["trade_emoji"] = trade_emoji

                            rem = get_cooldown_remaining(existing)
                            st = f"CD: {rem}s" if rem > 0 else "Ready"
                            ch_table.update_rows([{
                                "id": cid,
                                "cells": [
                                    {"text": ch.name, "imageUrl": str(g.icon.url) if g.icon else "", "subtext": g.name},
                                    {"text": f"{cooldown}s", "subtext": st},
                                    {"text": st, "subtext": (existing.get("last_sent") or "Never")[:19]},
                                    {}
                                ]
                            }])
                        else:
                            # New entry with actual cooldown
                            d["trade_channels"].append({
                                "id": cid,
                                "server_id": str(g.id),
                                "server_name": g.name,
                                "server_icon": str(g.icon.url) if g.icon else "",
                                "channel_name": ch.name,
                                "cooldown": cooldown,
                                "last_sent": None,
                                "trade_emoji": trade_emoji
                            })
                            
                            ch_table.insert_rows([{
                                "id": cid,
                                "cells": [
                                    {"text": ch.name, "imageUrl": str(g.icon.url) if g.icon else "", "subtext": g.name},
                                    {"text": f"{cooldown}s", "subtext": "Ready"},
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
                    "trade_emoji": await find_trade_emoji(g),
                    "cooldown_until": None
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
                rem = get_cooldown_remaining(c)
                if rem > 0:
                    skip += 1
                    print(f"[{idx}/{total}] Skipped {c['channel_name']} (cooldown: {rem}s)", type_="INFO")
                    continue
                
                msg = await build_msg(c["server_id"], d["trade_offers"], d["trade_requests"], c.get("trade_emoji"))
                ok, err = await send_to(c["id"], msg)
                
                if ok:
                    sent += 1
                    now = datetime.now()
                    c["last_sent"] = now.isoformat()
                    c["cooldown_until"] = None
                    print(f"[{idx}/{total}] ✓ {c['channel_name']}", type_="SUCCESS")
                else:
                    fail += 1
                    if isinstance(err, dict) and err.get("type") == "cooldown":
                        try:
                            retry_seconds = float(err.get("retry_after", 0))
                        except (TypeError, ValueError):
                            retry_seconds = 0
                        if retry_seconds > 0:
                            next_time = datetime.now() + timedelta(seconds=retry_seconds)
                            c["cooldown_until"] = next_time.isoformat()
                            print(f"[{idx}/{total}] ⏳ {c['channel_name']}: retry in {int(retry_seconds)}s", type_="WARNING")
                        else:
                            print(f"[{idx}/{total}] ✗ {c['channel_name']}: {describe_error(err)}", type_="ERROR")
                    else:
                        try:
                            cooldown_val = int(c.get("cooldown", 60))
                        except:
                            cooldown_val = 60
                        next_time = datetime.now() + timedelta(seconds=cooldown_val)
                        c["cooldown_until"] = next_time.isoformat()
                        print(f"[{idx}/{total}] ✗ {c['channel_name']}: {describe_error(err)}", type_="ERROR")

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

        error_log = []

        while AutoState.running:
            try:
                d = load_data()

                if not d["trade_offers"] or not d["trade_requests"] or not d["trade_channels"]:
                    await asyncio.sleep(5)
                    continue

                sent_this_loop = 0

                channels_with_cooldowns = []
                for c in d["trade_channels"]:
                    rem = get_cooldown_remaining(c)
                    channels_with_cooldowns.append((rem, c))

                channels_with_cooldowns.sort(key=lambda x: x[0])

                for rem, c in channels_with_cooldowns:
                    if not AutoState.running:
                        break

                    if rem <= 0:
                        try:
                            msg = await build_msg(c["server_id"], d["trade_offers"], d["trade_requests"], c.get("trade_emoji"))
                            ok, err = await send_to(c["id"], msg)

                            if ok:
                                now = datetime.now()
                                c["last_sent"] = now.isoformat()
                                c["cooldown_until"] = None
                                sent_this_loop += 1
                                print(f"✓ Auto: {c['channel_name']}", type_="SUCCESS")
                            else:
                                message = describe_error(err)
                                if isinstance(err, dict) and err.get("type") == "cooldown":
                                    try:
                                        retry_seconds = float(err.get("retry_after", 0))
                                    except (TypeError, ValueError):
                                        retry_seconds = 0

                                    if retry_seconds > 0:
                                        next_time = datetime.now() + timedelta(seconds=retry_seconds)
                                        c["cooldown_until"] = next_time.isoformat()
                                        print(f"⌛ Auto: {c['channel_name']} retry in {int(retry_seconds)}s", type_="WARNING")
                                    else:
                                        print(f"✗ Auto: {c['channel_name']}: {message}", type_="ERROR")
                                else:
                                    try:
                                        cooldown_val = int(c.get("cooldown", 60))
                                    except:
                                        cooldown_val = 60
                                    next_time = datetime.now() + timedelta(seconds=cooldown_val)
                                    c["cooldown_until"] = next_time.isoformat()
                                    print(f"✗ Auto: {c['channel_name']}: {message}", type_="ERROR")

                                error_log.append(f"{c['channel_name']}: {message}")

                            await asyncio.sleep(random.uniform(2, 4))
                        except Exception as e:
                            try:
                                cooldown_val = int(c.get("cooldown", 60))
                            except:
                                cooldown_val = 60
                            next_time = datetime.now() + timedelta(seconds=cooldown_val)
                            c["cooldown_until"] = next_time.isoformat()
                            print(f"✗ Auto: {c['channel_name']}: {str(e)}", type_="ERROR")
                            error_log.append(f"{c['channel_name']}: {str(e)}")

                save_data(d)

                min_wait = float('inf')
                for rem, _ in channels_with_cooldowns:
                    if rem < min_wait:
                        min_wait = rem

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
    test_btn.onClick = lambda: bot.loop.create_task(send_test_format())

    # Initialization
    async def init():
        d = load_data()
        
        for c in d["trade_channels"]:
            try:
                rem = get_cooldown_remaining(c)
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
