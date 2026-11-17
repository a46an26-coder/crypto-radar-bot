#!/usr/bin/env python3
"""
SUPER CRYPTO BOT 2026 - EARLY PUMP CATCHER v2
==============================================
- Параллельный WebSocket для всех пар (зависание одной не блокирует остальные)
- Логирование каждой свечи (видно, что бот получает данные)
- Сниженные пороги для теста детектора (потом подними)
"""
import ccxt
import ccxt.pro as ccxtpro
import asyncio
import time
import json
import os
import threading
import numpy as np
import logging
import requests
import hashlib
import hmac
from datetime import datetime
from flask import Flask, jsonify, request
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
BYBIT_API_KEY = os.getenv('BYBIT_API_KEY', '')
BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() == 'true'
BYBIT_TESTNET = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
DASH_HOST = os.getenv('DASH_HOST', '127.0.0.1')
DASH_TOKEN = os.getenv('DASH_TOKEN', '')
DASH_PORT = int(os.getenv('DASH_PORT', '5000'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET,
    'enableRateLimit': True, 'options': {'defaultType': 'spot'}
})
if BYBIT_TESTNET:
    exchange.set_sandbox_mode(True)

exchange_pro = ccxtpro.bybit({
    'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET,
    'enableRateLimit': True,
})

bot_instance = None
SCANNER_RUNNING = True
positions_cache = {}
ai_weights = {"TrendMaster": 1.0, "ScalperX": 1.0, "PatternHunter": 1.0,
              "VolumeWhale": 1.0, "RiskGuardian": 1.0}
training_buffer = []

WATCH_PAIRS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT',
    'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
    'TON/USDT', 'SUI/USDT', 'SHIB/USDT', 'LTC/USDT', 'BCH/USDT'
]

candle_buffers = {sym: [] for sym in WATCH_PAIRS}
buffer_lock = threading.Lock()

settings = {
    "stop_loss": 2.0, "take_profit": 8.0, "max_amount": 1.5,
    "auto_trade_enabled": True, "max_positions": 5,
    "min_volume_usdt": 500000,
    # EARLY PUMP DETECTOR (1m) — СНИЖЕНЫ ДЛЯ ТЕСТА
    "early_pump_enabled": True,
    "early_vol_mult": 1.5,        # было 4.0
    "early_change_1m": 0.2,       # было 0.8
    "early_change_2m": 0.4,       # было 1.5
    "early_rsi_max": 85,
    "early_rsi_min": 45,          # чуть мягче
    # ANTI-FOMO
    "max_change_24h": 50.0,
    # TRAILING
    "trailing_activation": 3.0,
    "trailing_tiers": [(10, 1.0), (5, 1.5), (3, 2.0), (0, 3.0)],
    # PARTIAL TP
    "partial_tp_enabled": True,
    "partial_levels": [(4, 0.3), (8, 0.3)],
}
SECTIONS_STATE_FILE = "sections_state.json"

# ==================== STATE ====================
def load_json(f, d):
    if os.path.exists(f):
        try:
            with open(f, 'r') as fp: return json.load(fp)
        except: pass
    return d

def save_json(f, d):
    with open(f, 'w') as fp: json.dump(d, fp, indent=2)

def load_state():
    global positions_cache, ai_weights
    positions_cache = load_json("positions_cache.json", {})
    ai_weights = load_json("ai_weights.json", ai_weights)

def save_state():
    save_json("positions_cache.json", positions_cache)
    save_json("ai_weights.json", ai_weights)

# ==================== BALANCE ====================
def get_unified_balance():
    if not BYBIT_API_KEY: return 0.0
    try:
        ts = str(int(time.time()*1000)); p = "accountType=UNIFIED&coin=USDT"
        sig = hmac.new(BYBIT_API_SECRET.encode(), (ts + BYBIT_API_KEY + "5000" + p).encode(), hashlib.sha256).hexdigest()
        url = "https://api-testnet.bybit.com" if BYBIT_TESTNET else "https://api.bybit.com"
        r = requests.get(url + "/v5/account/wallet-balance",
                         headers={'X-BAPI-API-KEY': BYBIT_API_KEY, 'X-BAPI-TIMESTAMP': ts,
                                  'X-BAPI-SIGN': sig, 'X-BAPI-RECV-WINDOW': '5000'},
                         params={'accountType': 'UNIFIED', 'coin': 'USDT'}, timeout=15)
        d = r.json()
        if d.get('retCode') == 0:
            for w in d['result']['list']:
                for c in w.get('coin', []):
                    if c.get('coin') == 'USDT': return float(c.get('walletBalance', 0))
        return 0.0
    except Exception as e:
        logger.error(f"Balance: {e}"); return 0.0

def get_full_balance():
    if not BYBIT_API_KEY: return 0.0, 0.0, 0.0
    usdt = get_unified_balance(); pos = 0.0
    try:
        b = exchange.fetch_balance()
        for c, a in b['total'].items():
            if a and a > 0 and c != 'USDT':
                t = exchange.fetch_ticker(f'{c}/USDT'); pos += a * t['last']
    except: pass
    return usdt + pos, usdt, pos

def get_positions_with_pnl():
    if not BYBIT_API_KEY: return []
    res = []
    try:
        b = exchange.fetch_balance()
        for c, a in b['total'].items():
            if a and a > 0 and c != 'USDT':
                t = exchange.fetch_ticker(f'{c}/USDT'); price = t['last']; val = a*price
                if val < 0.1: continue
                buy = positions_cache.get(f'{c}/USDT', {}).get('buy_price', price)
                pnl = ((price - buy)/buy)*100
                res.append({'symbol': f'{c}/USDT', 'qty': round(a,4), 'price': round(price,6),
                            'value': round(val,2), 'pnl': round(pnl,1), 'buy_price': buy})
    except: pass
    return res

# ==================== AI ====================
def calc_rsi(prices, period=14):
    if len(prices) < period+1: return 50.0
    d = np.diff(prices[-period-1:]); g = np.where(d>0, d, 0); l = np.where(d<0, -d, 0)
    ag = np.mean(g[:period]) if len(g) >= period else 0.1
    al = np.mean(l[:period]) if len(l) >= period else 0.1
    if al == 0: return 100.0
    return min(100, max(0, 100 - (100/(1+ag/al))))

def calc_atr(o, period=14):
    if len(o) < period+1: return 0.0
    tr = [max(o[i][2]-o[i][3], abs(o[i][2]-o[i-1][4]), abs(o[i][3]-o[i-1][4])) for i in range(1, len(o))]
    return np.mean(tr[-period:]) if tr else 0.0

def trend_master(sym):
    try:
        o = exchange.fetch_ohlcv(sym, '1h', limit=200); c = [x[4] for x in o]
        if len(c) < 50: return 0, 0
        e9, e21, e50 = np.mean(c[-9:]), np.mean(c[-21:]), np.mean(c[-50:])
        if e9 > e21 > e50: return 1, 75
        if e9 < e21 < e50: return -1, 75
        return 0, 30
    except: return 0, 0

def scalper_x(sym):
    try:
        o = exchange.fetch_ohlcv(sym, '15m', limit=100); c = [x[4] for x in o]
        if len(c) < 20: return 0, 0
        rsi = calc_rsi(c); ma = np.mean(c[-20:]); sd = np.std(c[-20:])
        if rsi < 30 and c[-1] < ma - 2*sd: return 1, 80
        if rsi > 70 and c[-1] > ma + 2*sd: return -1, 80
        return 0, 40
    except: return 0, 0

def pattern_hunter(sym):
    try:
        o = exchange.fetch_ohlcv(sym, '4h', limit=100)
        if len(o) < 3: return 0, 0
        c1, c2, c3 = o[-3], o[-2], o[-1]
        if c2[4]<c2[1] and c3[4]>c3[1] and c3[4]>c2[1] and c3[1]<c2[4]: return 1, 85
        if c2[4]>c2[1] and c3[4]<c3[1] and c3[4]<c2[1] and c3[1]>c2[4]: return -1, 85
        return 0, 35
    except: return 0, 0

def volume_whale(sym):
    try:
        o = exchange.fetch_ohlcv(sym, '1h', limit=100); v = [x[5] for x in o]
        if len(v) < 20: return 0, 0
        avg = np.mean(v[-20:-1])
        if avg == 0: return 0, 0
        r = v[-1]/avg
        if r > 2.0:
            if o[-1][4] > o[-2][4]: return 1, 80
            if o[-1][4] < o[-2][4]: return -1, 80
        return 0, 40
    except: return 0, 0

def risk_guardian(sym):
    try:
        o = exchange.fetch_ohlcv(sym, '1h', limit=100); atr = calc_atr(o)
        price = o[-1][4] if o else 0
        if price == 0: return 0, 0, 0, 0
        sl = min(5.0, (atr*1.5/price)*100); tp = min(12.0, (atr*3.0/price)*100)
        return 0, 50, max(1.5, sl), max(3.0, tp)
    except: return 0, 0, 2.0, 8.0

def ai_consensus(sym):
    s_list, c_list, w_list = [], [], []
    for name, fn in [("TrendMaster", trend_master), ("ScalperX", scalper_x),
                     ("PatternHunter", pattern_hunter), ("VolumeWhale", volume_whale)]:
        s, c = fn(sym); s_list.append(s); c_list.append(c); w_list.append(ai_weights.get(name, 1.0))
    _, _, sl, tp = risk_guardian(sym)
    tw = sum(w_list) or 1
    return (sum(s*w for s,w in zip(s_list,w_list))/tw,
            sum(c*w for c,w in zip(c_list,w_list))/tw, sl, tp)

# ==================== EARLY PUMP ====================
def detect_early_pump(sym, candles):
    if not settings['early_pump_enabled']: return False, 0, {}
    if len(candles) < 22: return False, 0, {}
    closes = [c[4] for c in candles]
    vols = [c[5] for c in candles]
    avg_vol = np.mean(vols[-21:-1]) or 1
    vol_ratio = vols[-1] / avg_vol
    change_1m = (closes[-1] - closes[-2]) / closes[-2] * 100
    change_2m = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) >= 3 else 0
    rsi_1m = calc_rsi(closes[-15:]) if len(closes) >= 15 else 50
    green_count = 0
    for i in range(-1, -4, -1):
        if closes[i] > candles[i][1]: green_count += 1
        else: break
    score = 0
    if vol_ratio >= settings['early_vol_mult']: score += 40
    elif vol_ratio >= settings['early_vol_mult'] * 0.7: score += 25
    if change_1m >= settings['early_change_1m']: score += 25
    if change_2m >= settings['early_change_2m']: score += 20
    if green_count >= 2: score += 15
    if settings['early_rsi_min'] <= rsi_1m <= settings['early_rsi_max']: score += 10
    is_early = (score >= 60 and
                vol_ratio >= settings['early_vol_mult'] * 0.7 and
                change_1m >= settings['early_change_1m'] and
                rsi_1m <= settings['early_rsi_max'])
    details = {'vol_ratio': round(vol_ratio, 2), 'change_1m': round(change_1m, 2),
               'change_2m': round(change_2m, 2), 'rsi_1m': round(rsi_1m, 1), 'green': green_count}
    return is_early, score, details

def detect_dump(sym, candles_1m):
    if len(candles_1m) < 5: return False
    c = [x[4] for x in candles_1m]; v = [x[5] for x in candles_1m]
    ch1 = (c[-1]-c[-2])/c[-2]*100
    ch3 = (c[-1]-c[-4])/c[-4]*100
    vr = v[-1] / (np.mean(v[-10:-1]) or 1)
    dd = (c[-1] - max(c[-5:])) / max(c[-5:]) * 100
    return (ch1 <= -1.0 and vr >= 1.5) or (ch3 <= -2.0) or (dd <= -3.0)

# ==================== TRADE ====================
def buy_token(sym, amount, reason=""):
    free = get_unified_balance()
    if amount > free: return False
    try:
        t = exchange.fetch_ticker(sym); price = t['last']
        qty = exchange.amount_to_precision(sym, amount/price)
        if DRY_RUN:
            logger.info(f"🔒 DRY: BUY {sym} @ ${price:.6f} ${amount}")
            send_telegram(f"🔒 *DRY*: BUY {sym}\n💰 ${price:.6f}\n💵 ${amount}\n📋 {reason}")
        else:
            exchange.create_market_buy_order(sym, qty)
            send_telegram(f"🟢 *ПОКУПКА {sym}*\n💰 ${price:.6f}\n💵 ${amount}\n📋 {reason}")
        positions_cache[sym] = {'buy_price': price, 'trailing_high': price,
                                'partial_levels': [], 'buy_time': time.time()}
        save_state(); return True
    except Exception as e:
        logger.error(f"Buy: {e}"); return False

def sell_token(sym, reason="", portion=1.0):
    try:
        coin = sym.split('/')[0]
        b = exchange.fetch_balance()
        amt = b.get(coin, {}).get('free', 0)
        if amt <= 0: return False, 0
        sell_amt = amt * portion
        t = exchange.fetch_ticker(sym); price = t['last']
        if DRY_RUN:
            logger.info(f"🔒 DRY: SELL {sym} x{portion} @ ${price:.6f} ({reason})")
            send_telegram(f"🔒 *DRY*: SELL {sym} x{portion:.0%}\n💰 ${price:.6f}\n📋 {reason}")
        else:
            exchange.create_market_sell_order(sym, exchange.amount_to_precision(sym, sell_amt))
            send_telegram(f"🔴 *ПРОДАЖА {sym}* x{portion:.0%}\n💰 ${price:.6f}\n📋 {reason}")
        buy = positions_cache.get(sym, {}).get('buy_price', price)
        pnl = ((price-buy)/buy)*100
        if portion >= 1.0:
            learn_from_trade(sym, buy, price)
            positions_cache.pop(sym, None); save_state()
        return True, pnl
    except Exception as e:
        logger.error(f"Sell: {e}"); return False, 0

def learn_from_trade(sym, buy, sell):
    pnl = ((sell-buy)/buy)*100
    training_buffer.append({"symbol": sym, "buy": buy, "sell": sell, "pnl": pnl, "time": time.time()})
    ai_weights["RiskGuardian"] = min(2.0, ai_weights["RiskGuardian"]*1.02) if pnl > 0 else max(0.5, ai_weights["RiskGuardian"]*0.98)
    save_state()

def check_positions():
    for p in get_positions_with_pnl():
        sym = p['symbol']; pnl = p['pnl']; price = p['price']
        cache = positions_cache.get(sym, {})
        if pnl <= -settings['stop_loss']:
            sell_token(sym, "стоп-лосс"); continue
        if pnl >= settings['take_profit']:
            sell_token(sym, "тейк-профит"); continue
        if pnl >= settings['trailing_activation']:
            high = cache.get('trailing_high', p['buy_price'])
            if price > high:
                cache['trailing_high'] = price; positions_cache[sym] = cache; save_state()
            trail = 3.0
            for lvl, dist in settings['trailing_tiers']:
                if pnl >= lvl: trail = dist; break
            if price <= high * (1 - trail/100):
                sell_token(sym, f"трейлинг-стоп {trail}%"); continue
        with buffer_lock:
            buf = list(candle_buffers.get(sym, []))
        if buf and detect_dump(sym, buf):
            sell_token(sym, "потеря высоты (dump)"); continue
        if settings['partial_tp_enabled']:
            reached = cache.get('partial_levels', [])
            for lvl, pct in settings['partial_levels']:
                if pnl >= lvl and lvl not in reached:
                    sell_token(sym, f"частичная фиксация +{lvl}%", portion=pct)
                    reached.append(lvl); cache['partial_levels'] = reached
                    positions_cache[sym] = cache; save_state()

# ==================== PARALLEL WEBSOCKET ====================
async def watch_one_pair(sym):
    """Каждая пара в своей задаче — зависание одной не блокирует остальные"""
    while True:
        try:
            candles = await exchange_pro.watch_ohlcv(sym, '1m', limit=30)
            if not candles: continue
            with buffer_lock:
                candle_buffers[sym] = candles[-30:]
            # Логируем каждую свечу, чтобы видеть, что данные идут
            logger.info(f"📊 {sym}: close={candles[-1][4]:.6f} vol={candles[-1][5]:.2f}")
            # Проверяем старт
            is_early, score, details = detect_early_pump(sym, candles)
            if is_early and SCANNER_RUNNING:
                try:
                    t = exchange.fetch_ticker(sym)
                    ch24 = t.get('percentage', 0) or 0
                except:
                    ch24 = 0
                if ch24 <= settings['max_change_24h']:
                    send_telegram(
                        f"🚀 *РАННИЙ СТАРТ* {sym}\n"
                        f"📊 1m: +{details['change_1m']}% | 2m: +{details['change_2m']}%\n"
                        f"📊 Объём: x{details['vol_ratio']} | RSI: {details['rsi_1m']}\n"
                        f"🧠 Скор: {score}/100"
                    )
                    if settings['auto_trade_enabled']:
                        am = min(settings['max_amount'], get_unified_balance())
                        if am >= 1:
                            buy_token(sym, am, f"Ранний старт {score}")
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"WS {sym}: {e}")
            await asyncio.sleep(5)

async def watch_all_pairs():
    tasks = [asyncio.create_task(watch_one_pair(sym)) for sym in WATCH_PAIRS]
    await asyncio.gather(*tasks)

async def positions_loop():
    """Отдельная задача для проверки позиций (SL/TP/trailing/dump)"""
    while True:
        try:
            check_positions()
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Positions loop: {e}")
            await asyncio.sleep(5)

async def main_async():
    await asyncio.gather(watch_all_pairs(), positions_loop())

def start_websocket_scanner():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_async())

# ==================== TELEGRAM ====================
def send_telegram(text, parse_mode='Markdown'):
    try:
        if bot_instance: bot_instance.send_message(chat_id=CHAT_ID, text=text[:4000], parse_mode=parse_mode)
    except: pass

def get_balance_text():
    if not BYBIT_API_KEY: return "⚠️ API-ключи Bybit не заданы"
    total, free, pos = get_full_balance()
    return f"💰 *ОБЩИЙ:* ${total:.2f}\n💵 *USDT:* ${free:.2f}\n📊 *В ПОЗИЦИЯХ:* ${pos:.2f}"

def get_positions_text():
    if not BYBIT_API_KEY: return "⚠️ Нужны API-ключи"
    ps = get_positions_with_pnl()
    if not ps: return "📭 Нет позиций"
    text = "📈 *ПОЗИЦИИ*\n\n"
    for p in ps:
        e = "🟢" if p['pnl'] >= 0 else "🔴"
        text += f"{e} {p['symbol']}: {p['qty']} шт, ${p['value']} ({p['pnl']:+.1f}%)\n"
    return text

def build_menu():
    state = load_json(SECTIONS_STATE_FILE, {})
    kb = []
    sections = {
        "info": ("📊 ИНФОРМАЦИЯ", ["bal", "pos"]),
        "trading": ("💰 ТОРГОВЛЯ", ["sellall", "toggle_auto"]),
        "market": ("📈 РЫНОК", ["top24"]),
        "ai": ("🧠 AI", ["ai_status"]),
        "system": ("🔧 СИСТЕМА", ["start", "stop"]),
    }
    names = {"bal":"💰 Баланс","pos":"📈 Позиции","sellall":"🔴 Продать всё",
             "toggle_auto":"🤖 Авто","top24":"🏆 Топ 24ч","ai_status":"🧠 AI статус",
             "start":"▶️ Старт","stop":"⏹️ Стоп"}
    for sid, (title, btns) in sections.items():
        exp = state.get(sid, True); icon = "▼" if exp else "▶"
        kb.append([InlineKeyboardButton(f"{icon} {title} {icon}", callback_data=f"t_{sid}")])
        if exp:
            row = []
            for b in btns:
                row.append(InlineKeyboardButton(names[b], callback_data=b))
                if len(row) == 2: kb.append(row); row = []
            if row: kb.append(row)
    return InlineKeyboardMarkup(kb)

def cmd_start(update, context):
    mode = "🔒 DRY RUN" if DRY_RUN else ("🧪 TESTNET" if BYBIT_TESTNET else "🔴 LIVE")
    text = f"🤖 *SUPER CRYPTO BOT 2026*\nРежим: *{mode}*\n🚀 Параллельный WebSocket 1m\n📊 Пороги (тест): vol x{settings['early_vol_mult']} | 1m +{settings['early_change_1m']}%\n\n👇 Выбери раздел"
    update.message.reply_text(text, parse_mode='Markdown', reply_markup=build_menu())

def callback_handler(update, context):
    global SCANNER_RUNNING
    q = update.callback_query; q.answer(); d = q.data
    if d.startswith("t_"):
        sid = d[2:]; st = load_json(SECTIONS_STATE_FILE, {})
        st[sid] = not st.get(sid, True); save_json(SECTIONS_STATE_FILE, st)
        q.edit_message_text("📋 Обновлено", reply_markup=build_menu()); return
    if d == "bal": q.edit_message_text(get_balance_text(), parse_mode='Markdown', reply_markup=build_menu())
    elif d == "pos": q.edit_message_text(get_positions_text(), parse_mode='Markdown', reply_markup=build_menu())
    elif d == "sellall":
        if not BYBIT_API_KEY: q.edit_message_text("⚠️ Нужны API-ключи", reply_markup=build_menu()); return
        for p in get_positions_with_pnl(): sell_token(p['symbol'], "продать всё")
        q.edit_message_text("✅ Готово", reply_markup=build_menu())
    elif d == "toggle_auto":
        settings['auto_trade_enabled'] = not settings['auto_trade_enabled']
        q.edit_message_text(f"✅ Авто: {settings['auto_trade_enabled']}", reply_markup=build_menu())
    elif d == "top24":
        try:
            tk = exchange.fetch_tickers()
            leaders = sorted([{'s': s, 'c': t.get('percentage', 0)} for s, t in tk.items()
                              if s.endswith('/USDT') and (t.get('percentage') or 0) > 0],
                             key=lambda x: x['c'], reverse=True)[:10]
            text = "🏆 *ТОП 24Ч*\n\n" + "\n".join([f"{i+1}. {x['s']}: +{x['c']:.1f}%" for i, x in enumerate(leaders)])
            q.edit_message_text(text, parse_mode='Markdown', reply_markup=build_menu())
        except: q.edit_message_text("🏆 Нет данных", reply_markup=build_menu())
    elif d == "ai_status":
        text = "🧠 *AI ВЕСА*\n\n" + "\n".join([f"• {k}: {v:.2f}" for k, v in ai_weights.items()])
        text += f"\n\n🚀 Ранний детектор: {'ON' if settings['early_pump_enabled'] else 'OFF'}"
        text += f"\n📊 Пороги: vol x{settings['early_vol_mult']} | 1m +{settings['early_change_1m']}% | 2m +{settings['early_change_2m']}%"
        q.edit_message_text(text, parse_mode='Markdown', reply_markup=build_menu())
    elif d == "start": SCANNER_RUNNING = True; q.edit_message_text("✅ Старт", reply_markup=build_menu())
    elif d == "stop": SCANNER_RUNNING = False; q.edit_message_text("⏹️ Стоп", reply_markup=build_menu())

def handle_message(update, context):
    update.message.reply_text("Используй /start", reply_markup=build_menu())

# ==================== DASHBOARD ====================
app = Flask(__name__)
def dash_auth():
    if not DASH_TOKEN: return True
    return request.args.get('token') == DASH_TOKEN or request.headers.get('X-Token') == DASH_TOKEN

@app.route('/')
def dash():
    if not dash_auth(): return "Unauthorized", 401
    total, free, pos = get_full_balance()
    mode = "DRY_RUN" if DRY_RUN else ("TESTNET" if BYBIT_TESTNET else "LIVE")
    return (f"<h1>🤖 SuperCryptoBot2026</h1>"
            f"<p>Mode: {mode}</p><p>Total: ${total:.2f}</p>"
            f"<p>USDT: ${free:.2f}</p><p>Positions: ${pos:.2f}</p>"
            f"<p>🚀 Early Pump: {'ON' if settings['early_pump_enabled'] else 'OFF'}</p>"
            f"<p>📊 Пороги: vol x{settings['early_vol_mult']} | 1m +{settings['early_change_1m']}%</p>")

@app.route('/health')
def health():
    return jsonify({"status": "ok", "mode": "DRY_RUN" if DRY_RUN else "LIVE",
                    "early_pump": settings['early_pump_enabled'],
                    "vol_mult": settings['early_vol_mult'],
                    "change_1m": settings['early_change_1m']})

def run_web():
    app.run(host=DASH_HOST, port=DASH_PORT, debug=False, threaded=True)

# ==================== MAIN ====================
def main():
    global bot_instance
    load_state()
    mode = "🔒 DRY RUN" if DRY_RUN else ("🧪 TESTNET" if BYBIT_TESTNET else "🔴 LIVE")
    print(f"\n🤖 SUPER CRYPTO BOT 2026 — режим: {mode}")
    print(f"🚀 Ранний детектор (параллельный WebSocket 1m): {'ON' if settings['early_pump_enabled'] else 'OFF'}")
    print(f"📊 Пороги (тест): vol x{settings['early_vol_mult']} | 1m +{settings['early_change_1m']}% | 2m +{settings['early_change_2m']}%")
    if not BYBIT_API_KEY: print("⚠️ API-ключи Bybit не заданы — торговля недоступна")
    if DRY_RUN: print("🔒 DRY RUN: ордера имитируются")

    threading.Thread(target=start_websocket_scanner, daemon=True).start()
    print("🔌 Параллельный WebSocket-сканер запущен (1m-свечи)")

    threading.Thread(target=run_web, daemon=True).start()
    print(f"🌐 Дашборд: http://{DASH_HOST}:{DASH_PORT}")

    updater = Updater(TOKEN, use_context=True)
    bot_instance = updater.bot
    dp = updater.dispatcher
    dp.add_handler(CommandHandler('start', cmd_start))
    dp.add_handler(CallbackQueryHandler(callback_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    updater.start_polling()
    print("✅ Бот запущен! /start в Telegram\n")
    updater.idle()

if __name__ == "__main__":
    main()
