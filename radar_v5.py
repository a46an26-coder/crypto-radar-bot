#!/usr/bin/env python3
"""
CRYPTO PUMP/DUMP RADAR v5.1
===========================
Запуск:  python3 radar_v5.py

Установка (venv уже активирован):
    pip install "ccxt>=4.2" "python-telegram-bot==13.15" flask requests python-dotenv

.env в ТОЙ ЖЕ папке, что и radar_v5.py:
    TELEGRAM_BOT_TOKEN=123456:ABC...
    CHAT_ID=123456789
    DRY_RUN=true
    PAPER_BALANCE=100
    # опционально:
    BYBIT_API_KEY=
    BYBIT_API_SECRET=
    BYBIT_TESTNET=false
    UNIVERSE_SIZE=40
    DASH_HOST=127.0.0.1
    DASH_PORT=5000
    DASH_TOKEN=

Важно:
  * ccxt.pro НЕ ставится отдельно — watch_trades входит в ccxt>=4.2.
  * DRY_RUN=true по умолчанию — бумажная торговля.
  * Не выключайте DRY_RUN без форвард-теста на testnet.
"""
import asyncio
import hmac
import html
import json
import logging
import os
import queue
import re
import sys
import threading
import time
from collections import deque

# --- ccxt.pro входит в ccxt с версии 4.x ---
try:
    import ccxt.pro as ccxtpro
    _HAVE_CCXTPRO = True
except ImportError:
    _HAVE_CCXTPRO = False
    ccxtpro = None

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import CallbackQueryHandler, CommandHandler, Filters, Updater

# ==================== КОНФИГ ====================
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
CHAT_ID = os.getenv('CHAT_ID', '').strip()
BYBIT_API_KEY = os.getenv('BYBIT_API_KEY', '').strip()
BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET', '').strip()
DRY_RUN = os.getenv('DRY_RUN', 'true').lower() == 'true'
BYBIT_TESTNET = os.getenv('BYBIT_TESTNET', 'false').lower() == 'true'
PAPER_BALANCE = float(os.getenv('PAPER_BALANCE', '100'))
UNIVERSE_SIZE = int(os.getenv('UNIVERSE_SIZE', '40'))
MAX_SYMBOLS = 80
DASH_HOST = os.getenv('DASH_HOST', '127.0.0.1')
DASH_PORT = int(os.getenv('DASH_PORT', '5000'))
DASH_TOKEN = os.getenv('DASH_TOKEN', '')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('radar')

FEE = 0.001
SLIP = 0.0005
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, 'state.json')

CORE_PAIRS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT',
    'ADA/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT', 'POL/USDT',
    'SUI/USDT', 'SHIB/USDT', 'LTC/USDT', 'BCH/USDT', 'NEAR/USDT',
]
STABLES = {'USDC', 'FDUSD', 'TUSD', 'USDE', 'DAI', 'USDD', 'PYUSD',
           'EUR', 'EURI', 'USD1', 'BUSD', 'USDP'}

PARTIALS = [(4.0, 0.3), (8.0, 0.3)]
TRAIL_TIERS = [(10.0, 1.0), (6.0, 1.5), (3.0, 2.0)]

cfg = {
    'alerts': True,
    'auto_trade': True,
    'pump_15s': 0.5, 'pump_60s': 1.0, 'pump_180s': 1.8,
    'dump_15s': 0.6, 'dump_60s': 1.2, 'dump_180s': 2.2,
    'vol_mult': 3.0,
    'min_vol_60s': 5000.0,
    'pressure': 0.6,
    'base_floor': 300.0,
    'warmup_sec': 180.0,
    'eval_interval': 0.5,
    'alert_cooldown': 120.0,
    'buy_cooldown': 900.0,
    'trade_amount': 10.0,
    'max_positions': 3,
    'stop_loss': 2.0,
    'take_profit': 15.0,
    'partial_tp': True,
    'max_change_24h': 50.0,
    'min_volume_usdt': 500000.0,
}

LOOP = asyncio.new_event_loop()
if _HAVE_CCXTPRO:
    ex = ccxtpro.bybit({
        'apiKey': BYBIT_API_KEY, 'secret': BYBIT_API_SECRET,
        'enableRateLimit': True, 'options': {'defaultType': 'spot'},
    })
    if BYBIT_TESTNET:
        ex.set_sandbox_mode(True)
else:
    ex = None

state = {'positions': {}, 'paper_usdt': PAPER_BALANCE}
tapes = {}
watchers = {}
ticker24 = {}
busy = set()
last_eval, last_alert, last_buy = {}, {}, {}
BG = set()
STATS = {'started': time.time(), 'last_msg': time.time(), 'trades': 0, 'signals': 0}
scanner_on = True
CHAT_ID_INT = 0


# ==================== УТИЛИТЫ ====================
def fp(p):
    return f"{p:.4f}" if p >= 1 else f"{p:.8f}"


def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            saved = json.load(f)
        state['positions'] = saved.get('positions', {})
        state['paper_usdt'] = saved.get('paper_usdt', PAPER_BALANCE)
        for k, v in saved.get('cfg', {}).items():
            if k in cfg and isinstance(v, type(cfg[k])):
                cfg[k] = v
        logger.info(f"state loaded: positions={len(state['positions'])}, paper_usdt={state['paper_usdt']}")
    except FileNotFoundError:
        logger.info("state.json не найден — старт с чистыми настройками")
    except Exception as e:
        logger.error(f"load_state: {e}")


def save_state():
    try:
        data = {'positions': {k: v for k, v in state['positions'].items() if not v.get('pending')},
                'paper_usdt': state['paper_usdt'], 'cfg': cfg}
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.error(f"save_state: {e}")


def spawn(coro):
    t = asyncio.ensure_future(coro, loop=LOOP)
    BG.add(t)

    def _done(f):
        BG.discard(f)
        if not f.cancelled() and f.exception():
            logger.error(f"task error: {f.exception()!r}")
    t.add_done_callback(_done)


def run_async(coro, timeout=25):
    return asyncio.run_coroutine_threadsafe(coro, LOOP).result(timeout)


def min_cost(sym):
    try:
        return float(ex.market(sym)['limits']['cost']['min'] or 1.0)
    except Exception:
        return 1.0


# ==================== TELEGRAM ====================
_tg_q = queue.Queue()


def tg(text):
    _tg_q.put(text)


def _tg_worker():
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    while True:
        text = _tg_q.get()
        try:
            for attempt in range(3):
                try:
                    r = requests.post(url, json={
                        'chat_id': CHAT_ID, 'text': text[:4000],
                        'parse_mode': 'HTML', 'disable_web_page_preview': True,
                    }, timeout=10)
                    if r.status_code == 200:
                        logger.debug("Telegram OK")
                        break
                    if r.status_code == 429:
                        ra = r.json().get('parameters', {}).get('retry_after', 3) + 1
                        logger.warning(f"Telegram rate limit, sleep {ra}s")
                        time.sleep(ra)
                        continue
                    logger.error(f"Telegram {r.status_code}: {r.text[:200]}")
                    break
                except requests.exceptions.RequestException as e:
                    logger.error(f"Telegram send attempt {attempt + 1}: {e}")
                    time.sleep(2)
        except Exception:
            logger.exception("tg worker fatal — сообщение потеряно")
        finally:
            _tg_q.task_done()


# ==================== ЛЕНТА И АНАЛИЗ ====================
class Tape:
    def __init__(self, keep=960):
        self.b = deque()
        self.keep = keep
        self.seen_q = deque(maxlen=3000)
        self.seen_set = set()

    def is_new(self, tid):
        if tid is None:
            return True
        if tid in self.seen_set:
            return False
        if len(self.seen_q) == self.seen_q.maxlen:
            self.seen_set.discard(self.seen_q[0])
        self.seen_q.append(tid)
        self.seen_set.add(tid)
        return True

    def add(self, price, cost, side):
        sec = int(time.time())
        buy = cost if side == 'buy' else 0.0
        if self.b and self.b[-1][0] == sec:
            x = self.b[-1]
            x[1] = price
            x[2] += cost
            x[3] += buy
        else:
            self.b.append([sec, price, cost, buy])
            while self.b and self.b[0][0] < sec - self.keep:
                self.b.popleft()

    def last_price(self):
        return self.b[-1][1] if self.b else None


def analyze(tape, now):
    b = tape.b
    if len(b) < 5 or now - b[0][0] < cfg['warmup_sec']:
        return None
    price = b[-1][1]
    ref = {15: None, 60: None, 180: None}
    vol60 = buy60 = base = 0.0
    for sec, close, vol, buy in reversed(b):
        age = now - sec
        if age < 60:
            vol60 += vol
            buy60 += buy
        elif age <= 900:
            base += vol
        for w in ref:
            if ref[w] is None and age >= w:
                ref[w] = close
    span = min(840.0, now - b[0][0] - 60)
    if span < 60:
        return None
    base_pm = base / span * 60
    return {
        'price': price,
        'ret': {w: ((price / ref[w] - 1) * 100 if ref[w] else None) for w in ref},
        'vol60': vol60,
        'ratio': vol60 / max(base_pm, cfg['base_floor']),
        'buy_share': buy60 / vol60 if vol60 > 0 else 0.5,
    }


def classify(m):
    if m['vol60'] < cfg['min_vol_60s'] or m['ratio'] < cfg['vol_mult']:
        return None
    for w in (15, 60, 180):
        r = m['ret'][w]
        if r is None:
            continue
        if r >= cfg[f'pump_{w}s'] and m['buy_share'] >= cfg['pressure']:
            return 'pump'
        if r <= -cfg[f'dump_{w}s'] and (1 - m['buy_share']) >= cfg['pressure']:
            return 'dump'
    return None


def alert_text(sym, sig, m):
    icon, title = ('🚀', 'PUMP') if sig == 'pump' else ('🔻', 'DUMP')
    parts = [f"{w}с {m['ret'][w]:+.2f}%" for w in (15, 60, 180) if m['ret'][w] is not None]
    share = m['buy_share'] if sig == 'pump' else 1 - m['buy_share']
    side = 'покупки' if sig == 'pump' else 'продажи'
    ch24 = ticker24.get(sym)
    pos = state['positions'].get(sym)
    t = (f"{icon} <b>{title} {sym}</b>\n"
         f"💰 {fp(m['price'])}\n"
         f"📈 {' | '.join(parts) if parts else 'нет данных'}\n"
         f"📊 Объём 60с: ${m['vol60']:,.0f} (x{m['ratio']:.1f} от обычного)\n"
         f"⚖️ Перевес {side}: {share * 100:.0f}%")
    if ch24 is not None:
        t += f"\n🕐 За 24ч: {ch24:+.1f}%"
    if pos and not pos.get('pending'):
        t += "\n📌 <i>по этой паре у бота открыта позиция</i>"
    return t


# ==================== ТОРГОВЛЯ ====================
def maybe_buy(sym, m):
    pos = state['positions']
    now = time.time()
    if sym in busy or sym in pos or len(pos) >= cfg['max_positions']:
        return
    if now - last_buy.get(sym, 0) < cfg['buy_cooldown']:
        return
    ch24 = ticker24.get(sym)
    if ch24 is not None and ch24 > cfg['max_change_24h']:
        return
    busy.add(sym)
    pos[sym] = {'pending': True}
    last_buy[sym] = now
    spawn(_buy(sym, m))


async def _place_buy(sym, usdt):
    if hasattr(ex, 'create_market_buy_order_with_cost'):
        return await ex.create_market_buy_order_with_cost(sym, usdt)
    return await ex.create_order(sym, 'market', 'buy', usdt, None,
                                 {'createMarketBuyOrderRequiresPrice': False})


async def _buy(sym, m):
    try:
        paper = DRY_RUN
        if paper:
            free = state['paper_usdt']
        else:
            bal = await ex.fetch_balance()
            free = float((bal.get('USDT') or {}).get('free') or 0)
        usdt = min(cfg['trade_amount'], free * 0.99)
        if usdt < min_cost(sym):
            tg(f"⚠️ {sym}: сумма ${usdt:.2f} меньше минимального ордера биржи / не хватает USDT")
            return

        if paper:
            fill = m['price'] * (1 + SLIP)
            qty = usdt / fill * (1 - FEE)
            state['paper_usdt'] -= usdt
        else:
            order = await _place_buy(sym, usdt)
            filled = order.get('filled')
            fill = order.get('average')
            if not filled or not fill:
                await asyncio.sleep(0.7)
                try:
                    order = await ex.fetch_order(order['id'], sym, params={'acknowledged': True})
                    filled = order.get('filled') or filled
                    fill = order.get('average') or fill
                except Exception as e:
                    logger.warning(f"fetch_order: {e}")
            if not fill:
                fill = m['price']
            if not filled:
                filled = usdt / fill
            qty = filled * (1 - FEE)

        state['positions'][sym] = {
            'buy_price': fill, 'qty': qty, 'qty0': qty, 'high': fill,
            'partials': [], 'time': time.time(), 'paper': paper, 'cost': usdt,
        }
        save_state()
        tg(f"{'🔒 DRY' if paper else '🟢'} <b>ПОКУПКА {sym}</b>\n💰 {fp(fill)}\n💵 ${usdt:.2f}\n"
           f"📋 сигнал PUMP (60с {(m['ret'][60] or 0):+.2f}%, x{m['ratio']:.1f} объём)")
        if sym not in watchers:
            start_watch(sym)
    except Exception as e:
        logger.exception("buy failed")
        tg(f"❌ Ошибка покупки {sym}: {html.escape(str(e))[:300]}")
    finally:
        p = state['positions'].get(sym)
        if p and p.get('pending'):
            state['positions'].pop(sym, None)
        busy.discard(sym)


def spawn_close(sym, reason, share, hint, level=None):
    if sym in busy:
        return
    busy.add(sym)
    spawn(_close(sym, reason, share, hint, level))


async def _close(sym, reason, share, hint, level=None):
    pos = state['positions'].get(sym)
    if not pos or pos.get('pending'):
        busy.discard(sym)
        return
    try:
        tape = tapes.get(sym)
        price = (tape.last_price() if tape else None) or hint
        full = share is None
        qty = pos['qty'] if full else min(pos['qty'], pos['qty0'] * share)
        if qty <= 0:
            busy.discard(sym)
            return

        if not pos['paper']:
            coin = sym.split('/')[0]
            bal = await ex.fetch_balance()
            free = float((bal.get(coin) or {}).get('free') or 0)
            qty = min(qty, free)
            if qty * price < min_cost(sym) * 1.02:
                if full:
                    state['positions'].pop(sym, None)
                    save_state()
                    tg(f"⚠️ {sym}: остаток слишком мал для продажи (или уже продан вручную) — позиция снята с учёта")
                else:
                    logger.info(f"{sym}: частичка {share:.0%} отложена — баланс ещё не обновился, повтор позже")
                return
            amt = float(ex.amount_to_precision(sym, qty))
            order = await ex.create_market_sell_order(sym, amt)
            fill = order.get('average') or price
            qty = amt
        else:
            fill = price * (1 - SLIP)
            state['paper_usdt'] += qty * fill * (1 - FEE)

        pnl = (fill / pos['buy_price'] - 1) * 100
        pos['qty'] -= qty
        if level is not None:
            pos['partials'].append(level)
        if full or pos['qty'] <= 1e-12:
            state['positions'].pop(sym, None)
            last_buy[sym] = time.time()
        save_state()
        part = 'ПОЛНАЯ' if full else f"ЧАСТИЧНАЯ {share:.0%}"
        tg(f"{'🔒 DRY' if pos['paper'] else '🔴'} <b>ПРОДАЖА {sym}</b> ({part})\n💰 {fp(fill)}\n"
           f"{'🟢' if pnl >= 0 else '🔴'} PnL: {pnl:+.2f}% (до комиссий)\n📋 {reason}")
    except Exception as e:
        logger.exception("close failed")
        pos = state['positions'].get(sym)
        if pos:
            pos['fails'] = pos.get('fails', 0) + 1
            pos['retry_at'] = time.time() + 10 * pos['fails']
            if pos['fails'] <= 3:
                tg(f"❌ Ошибка продажи {sym}: {html.escape(str(e))[:300]}\nПовторю через {10 * pos['fails']}с")
    finally:
        busy.discard(sym)


def manage_position(sym, price, now):
    pos = state['positions'].get(sym)
    if not pos or pos.get('pending') or sym in busy or now < pos.get('retry_at', 0):
        return
    bp = pos['buy_price']
    if price > pos['high']:
        pos['high'] = price
    pnl = (price / bp - 1) * 100
    max_pnl = (pos['high'] / bp - 1) * 100
    if pnl <= -cfg['stop_loss']:
        return spawn_close(sym, f"стоп-лосс {pnl:+.2f}%", None, price)
    if pnl >= cfg['take_profit']:
        return spawn_close(sym, f"тейк-профит {pnl:+.2f}%", None, price)
    if cfg['partial_tp']:
        for lvl, share in PARTIALS:
            if pnl >= lvl and lvl not in pos['partials']:
                return spawn_close(sym, f"частичная фиксация +{lvl}%", share, price, level=lvl)
    for lvl, dist in TRAIL_TIERS:
        if max_pnl >= lvl:
            if price <= pos['high'] * (1 - dist / 100):
                spawn_close(sym, f"трейлинг-стоп {dist}% (макс. +{max_pnl:.1f}%)", None, price)
            break


def on_tick(sym):
    tape = tapes[sym]
    price = tape.last_price()
    if price is None:
        return
    now = time.time()
    pos = state['positions'].get(sym)
    if pos and not pos.get('pending'):
        manage_position(sym, price, now)
    if now - last_eval.get(sym, 0) < cfg['eval_interval']:
        return
    last_eval[sym] = now
    m = analyze(tape, now)
    if not m:
        return
    sig = classify(m)
    if not sig:
        return
    pos = state['positions'].get(sym)
    if sig == 'dump' and pos and not pos.get('pending'):
        spawn_close(sym, 'сигнал DUMP (потеря высоты)', None, price)
    if not scanner_on:
        return
    key = (sym, sig)
    if cfg['alerts'] and now - last_alert.get(key, 0) >= cfg['alert_cooldown']:
        last_alert[key] = now
        STATS['signals'] += 1
        tg(alert_text(sym, sig, m))
    if sig == 'pump' and cfg['auto_trade']:
        maybe_buy(sym, m)


# ==================== WEBSOCKET ====================
async def watch_trades_loop(sym):
    tape = tapes.setdefault(sym, Tape())
    fails = 0
    while True:
        try:
            trades = await ex.watch_trades(sym)
            fails = 0
            STATS['last_msg'] = time.time()
            for t in trades:
                price = t.get('price')
                if not price or not tape.is_new(t.get('id')):
                    continue
                cost = t.get('cost') or price * (t.get('amount') or 0)
                tape.add(price, cost, t.get('side'))
                STATS['trades'] += 1
            on_tick(sym)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            fails += 1
            logger.error(f"WS {sym}: {e}")
            await asyncio.sleep(min(30, 2 ** fails))


def start_watch(sym):
    if not _HAVE_CCXTPRO:
        return
    if sym in watchers or len(watchers) >= MAX_SYMBOLS:
        return
    watchers[sym] = asyncio.ensure_future(watch_trades_loop(sym), loop=LOOP)


async def refresh_universe(first=False):
    tk = await ex.fetch_tickers()
    rows, movers = [], []
    for s, t in tk.items():
        if not s.endswith('/USDT') or ':' in s:
            continue
        base = s.split('/')[0]
        if base in STABLES or re.search(r'\d+[LS]$', base):
            continue
        pct = t.get('percentage')
        if pct is not None:
            ticker24[s] = float(pct)
        qv = t.get('quoteVolume') or 0
        if qv >= cfg['min_volume_usdt']:
            rows.append((qv, s))
            movers.append((abs(pct or 0), s))
    rows.sort(reverse=True)
    movers.sort(reverse=True)
    picks = [s for _, s in rows[:UNIVERSE_SIZE]] + [s for _, s in movers[:10]]
    added = 0
    for s in picks:
        if s not in watchers and len(watchers) < MAX_SYMBOLS:
            start_watch(s)
            added += 1
    if added and not first:
        logger.info(f"Добавлено пар: {added}, всего {len(watchers)}")


async def universe_loop():
    while True:
        await asyncio.sleep(120)
        try:
            await refresh_universe()
        except Exception as e:
            logger.error(f"universe: {e}")


async def maintenance_loop():
    while True:
        await asyncio.sleep(5)
        save_state()


async def watchdog():
    warned = False
    while True:
        await asyncio.sleep(15)
        age = time.time() - STATS['last_msg']
        if age > 60:
            logger.warning(f"Нет данных {age:.0f}с — переподключаю WebSocket")
            if not warned:
                tg("⚠️ Нет данных от биржи >60с, переподключаюсь")
                warned = True
            try:
                await ex.close()
            except Exception as e:
                logger.error(f"close: {e}")
            STATS['last_msg'] = time.time()
        elif age < 30:
            warned = False


async def main_async():
    await ex.load_markets()
    missing = [s for s in CORE_PAIRS if s not in ex.markets]
    if missing:
        logger.warning(f"CORE_PAIRS отсутствуют на бирже: {missing}")
    for s in CORE_PAIRS:
        if s in ex.markets:
            start_watch(s)
    for s in list(state['positions']):
        start_watch(s)
    try:
        await refresh_universe(first=True)
    except Exception as e:
        logger.error(f"universe start: {e}")
    mode = "DRY RUN (бумажная)" if DRY_RUN else ("TESTNET" if BYBIT_TESTNET else "LIVE")
    tg(f"🤖 <b>Radar v5.1 запущен</b>\nРежим: <b>{mode}</b>\nПар в наблюдении: {len(watchers)}\n"
       f"Сигналы начнутся через ~{int(cfg['warmup_sec'])}с (сбор базы объёма)\n/start — меню")
    await asyncio.gather(universe_loop(), maintenance_loop(), watchdog())


def loop_thread():
    asyncio.set_event_loop(LOOP)
    try:
        LOOP.run_until_complete(main_async())
    except Exception:
        logger.exception("main_async crashed")
        tg("🛑 Критическая ошибка основного цикла, перезапусти бота")


# ==================== ТЕКСТЫ МЕНЮ ====================
def pos_value():
    v = 0.0
    for sym, p in state['positions'].items():
        if p.get('pending'):
            continue
        t = tapes.get(sym)
        v += p['qty'] * ((t.last_price() if t else None) or p['buy_price'])
    return v


async def balance_text():
    pv = pos_value()
    if DRY_RUN:
        free = state['paper_usdt']
        return (f"🔒 <b>БУМАЖНЫЙ СЧЁТ</b>\n💰 Общий: ${free + pv:.2f}\n"
                f"💵 USDT: ${free:.2f}\n📊 В позициях: ${pv:.2f}")
    if not BYBIT_API_KEY:
        return "⚠️ API-ключи Bybit не заданы"
    bal = await ex.fetch_balance()
    usdt = bal.get('USDT') or {}
    free, total = float(usdt.get('free') or 0), float(usdt.get('total') or 0)
    return f"💰 <b>USDT всего:</b> ${total:.2f}\n💵 <b>Свободно:</b> ${free:.2f}\n📊 <b>Позиции бота:</b> ${pv:.2f}"


async def positions_text():
    if not state['positions']:
        return "📭 Нет открытых позиций бота"
    text = "📈 <b>ПОЗИЦИИ БОТА</b>\n\n"
    for sym, p in state['positions'].items():
        if p.get('pending'):
            text += f"⏳ {sym}: открывается...\n"
            continue
        t = tapes.get(sym)
        price = (t.last_price() if t else None) or p['buy_price']
        pnl = (price / p['buy_price'] - 1) * 100
        text += (f"{'🟢' if pnl >= 0 else '🔴'} {sym}{' 🔒' if p['paper'] else ''}: "
                 f"{p['qty']:.6g} шт, вход {fp(p['buy_price'])}, сейчас {fp(price)} ({pnl:+.2f}%)\n")
    return text


async def top24_text():
    tk = await ex.fetch_tickers()
    rows = sorted(((t.get('percentage') or 0, s) for s, t in tk.items()
                   if s.endswith('/USDT') and ':' not in s
                   and (t.get('quoteVolume') or 0) >= cfg['min_volume_usdt']),
                  reverse=True)[:10]
    if not rows:
        return "🏆 Нет данных"
    return "🏆 <b>ТОП 24Ч</b>\n\n" + "\n".join(f"{i + 1}. {s}: {c:+.1f}%" for i, (c, s) in enumerate(rows))


async def status_text():
    age = time.time() - STATS['last_msg']
    up = int(time.time() - STATS['started'])
    mode = "DRY RUN" if DRY_RUN else ("TESTNET" if BYBIT_TESTNET else "LIVE")
    return (f"🧠 <b>СТАТУС</b>\nРежим: {mode}\n"
            f"Сканер: {'ON' if scanner_on else 'OFF'} | Авто: {'ON' if cfg['auto_trade'] else 'OFF'}"
            f" | Алерты: {'ON' if cfg['alerts'] else 'OFF'}\n"
            f"Пар: {len(watchers)} | сделок: {STATS['trades']:,} | сигналов: {STATS['signals']}\n"
            f"Последние данные: {age:.0f}с | аптайм {up // 3600}ч {up % 3600 // 60}м\n\n"
            f"PUMP: 15с +{cfg['pump_15s']}% | 60с +{cfg['pump_60s']}% | 180с +{cfg['pump_180s']}%\n"
            f"DUMP: 15с -{cfg['dump_15s']}% | 60с -{cfg['dump_60s']}% | 180с -{cfg['dump_180s']}%\n"
            f"Объём x{cfg['vol_mult']} | мин. ${cfg['min_vol_60s']:.0f}/60с | перевес {cfg['pressure']:.0%}\n"
            f"Сделка ${cfg['trade_amount']} | SL {cfg['stop_loss']}% | макс. позиций {cfg['max_positions']}\n"
            f"Изменить: /set ключ значение, список: /cfg")


async def sell_all():
    n = 0
    for sym in list(state['positions']):
        p = state['positions'].get(sym)
        if p and not p.get('pending') and sym not in busy:
            busy.add(sym)
            await _close(sym, 'продать всё (вручную)', None, p['buy_price'])
            n += 1
    return n


# ==================== TELEGRAM-МЕНЮ ====================
menu_open = {}


def build_menu():
    kb = []
    sections = {
        "info": ("📊 ИНФОРМАЦИЯ", ["bal", "pos", "status"]),
        "trading": ("💰 ТОРГОВЛЯ", ["sellall", "toggle_auto", "toggle_alerts"]),
        "market": ("📈 РЫНОК", ["top24"]),
        "system": ("🔧 СИСТЕМА", ["start", "stop"]),
    }
    names = {"bal": "💰 Баланс", "pos": "📈 Позиции", "status": "🧠 Статус",
             "sellall": "🔴 Продать всё", "toggle_auto": "🤖 Авто вкл/выкл",
             "toggle_alerts": "🔔 Алерты вкл/выкл", "top24": "🏆 Топ 24ч",
             "start": "▶️ Старт", "stop": "⏹️ Стоп"}
    for sid, (title, btns) in sections.items():
        exp = menu_open.get(sid, True)
        icon = "▼" if exp else "▶"
        kb.append([InlineKeyboardButton(f"{icon} {title} {icon}", callback_data=f"t_{sid}")])
        if exp:
            row = []
            for b in btns:
                row.append(InlineKeyboardButton(names[b], callback_data=b))
                if len(row) == 2:
                    kb.append(row)
                    row = []
            if row:
                kb.append(row)
    return InlineKeyboardMarkup(kb)


def safe_edit(q, text, markup=None):
    try:
        q.edit_message_text(text, parse_mode='HTML', reply_markup=markup or build_menu())
    except BadRequest as e:
        if 'not modified' not in str(e).lower():
            logger.warning(f"edit: {e}")


def cmd_start(update, context):
    mode = "🔒 DRY RUN" if DRY_RUN else ("🧪 TESTNET" if BYBIT_TESTNET else "🔴 LIVE")
    update.message.reply_text(f"🤖 <b>CRYPTO RADAR v5.1</b>\nРежим: <b>{mode}</b>\n👇 Выбери раздел",
                              parse_mode='HTML', reply_markup=build_menu())


def cmd_cfg(update, context):
    update.message.reply_text("<pre>" + "\n".join(f"{k} = {v}" for k, v in cfg.items()) + "</pre>",
                              parse_mode='HTML')


def cmd_set(update, context):
    if len(context.args) != 2 or context.args[0] not in cfg:
        update.message.reply_text("Использование: /set ключ значение\nСписок ключей: /cfg")
        return
    k, v = context.args
    try:
        new = (v.lower() in ('1', 'true', 'on', 'yes')) if isinstance(cfg[k], bool) else type(cfg[k])(v)
    except ValueError:
        update.message.reply_text("❌ Неверное значение")
        return
    cfg[k] = new
    save_state()
    update.message.reply_text(f"✅ {k} = {new}")


def callback_handler(update, context):
    global scanner_on
    q = update.callback_query
    q.answer()
    if q.message.chat_id != CHAT_ID_INT:
        return
    d = q.data
    try:
        if d.startswith("t_"):
            sid = d[2:]
            if sid != 'none':
                menu_open[sid] = not menu_open.get(sid, True)
            safe_edit(q, "📋 Меню")
        elif d == "bal":
            safe_edit(q, run_async(balance_text()))
        elif d == "pos":
            safe_edit(q, run_async(positions_text()))
        elif d == "status":
            safe_edit(q, run_async(status_text()))
        elif d == "top24":
            safe_edit(q, run_async(top24_text()))
        elif d == "sellall":
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Да, продать всё", callback_data="sellall_yes"),
                                        InlineKeyboardButton("↩️ Отмена", callback_data="t_none")]])
            safe_edit(q, "⚠️ Продать ВСЕ позиции бота?", kb)
        elif d == "sellall_yes":
            n = run_async(sell_all(), timeout=60)
            safe_edit(q, f"✅ Закрыто позиций: {n}")
        elif d == "toggle_auto":
            cfg['auto_trade'] = not cfg['auto_trade']
            save_state()
            safe_edit(q, f"✅ Автоторговля: {'ON' if cfg['auto_trade'] else 'OFF'}")
        elif d == "toggle_alerts":
            cfg['alerts'] = not cfg['alerts']
            save_state()
            safe_edit(q, f"✅ Алерты: {'ON' if cfg['alerts'] else 'OFF'}")
        elif d == "start":
            scanner_on = True
            safe_edit(q, "✅ Сканер включён")
        elif d == "stop":
            scanner_on = False
            safe_edit(q, "⏹️ Сканер выключен (открытые позиции продолжают сопровождаться)")
    except Exception as e:
        logger.exception("callback")
        safe_edit(q, f"❌ Ошибка: {html.escape(str(e))[:200]}")


# ==================== DASHBOARD ====================
app = Flask(__name__)


def dash_auth():
    if not DASH_TOKEN:
        return True
    given = request.args.get('token') or request.headers.get('X-Token') or ''
    return hmac.compare_digest(given, DASH_TOKEN)


@app.route('/')
def dash():
    if not dash_auth():
        return "Unauthorized", 401
    return (f"<h1>🤖 Crypto Radar v5.1</h1>"
            f"<p>Mode: {'DRY_RUN' if DRY_RUN else 'LIVE'}</p>"
            f"<p>Pairs: {len(watchers)} | trades: {STATS['trades']} | signals: {STATS['signals']}</p>"
            f"<p>Open positions: {len(state['positions'])}</p>")


@app.route('/health')
def health():
    if not dash_auth():
        return "Unauthorized", 401
    return jsonify({"status": "ok", "mode": "DRY_RUN" if DRY_RUN else "LIVE",
                    "pairs": len(watchers),
                    "last_data_age_sec": round(time.time() - STATS['last_msg'], 1),
                    "signals": STATS['signals'], "positions": len(state['positions'])})


def run_web():
    if not DASH_TOKEN:
        logger.warning("DASH_TOKEN не задан — дашборд доступен без авторизации (не публикуй наружу)")
    app.run(host=DASH_HOST, port=DASH_PORT, debug=False, threaded=True, use_reloader=False)


# ==================== MAIN ====================
def main():
    global CHAT_ID_INT
    if not _HAVE_CCXTPRO:
        print("❌ Не установлен ccxt с поддержкой WebSocket (watch_trades).")
        print("   Установи:  pip install \"ccxt>=4.2\"")
        print("   Проверь:   python -c \"import ccxt.pro; print('OK')\"")
        sys.exit(1)
    if not TOKEN or not CHAT_ID:
        print("❌ В .env не заданы TELEGRAM_BOT_TOKEN и/или CHAT_ID.")
        print(f"   Ожидаемый файл .env: {os.path.join(BASE_DIR, '.env')}")
        sys.exit(1)
    try:
        CHAT_ID_INT = int(CHAT_ID)
    except ValueError:
        print(f"❌ CHAT_ID должен быть числом, получено: {CHAT_ID!r}")
        sys.exit(1)
    if not DRY_RUN and not BYBIT_API_KEY:
        print("❌ DRY_RUN=false требует BYBIT_API_KEY / BYBIT_API_SECRET в .env")
        sys.exit(1)

    load_state()
    print(f"🤖 CRYPTO RADAR v5.1 — режим: {'DRY RUN' if DRY_RUN else ('TESTNET' if BYBIT_TESTNET else 'LIVE')}")
    print(f"📁 state: {STATE_FILE}")

    threading.Thread(target=_tg_worker, daemon=True).start()
    threading.Thread(target=loop_thread, daemon=True).start()
    threading.Thread(target=run_web, daemon=True).start()
    print(f"🌐 Дашборд: http://{DASH_HOST}:{DASH_PORT}")

    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    only_me = Filters.chat(chat_id=CHAT_ID_INT)
    dp.add_handler(CommandHandler('start', cmd_start, filters=only_me))
    dp.add_handler(CommandHandler('cfg', cmd_cfg, filters=only_me))
    dp.add_handler(CommandHandler('set', cmd_set, filters=only_me, pass_args=True))
    dp.add_handler(CallbackQueryHandler(callback_handler))
    updater.start_polling()
    print("✅ Бот запущен! Открой Telegram и отправь /start")
    try:
        updater.idle()
    finally:
        save_state()
        try:
            for t in list(BG):
                t.cancel()
            asyncio.run_coroutine_threadsafe(ex.close(), LOOP).result(5)
        except Exception as e:
            logger.warning(f"shutdown ex.close: {e}")
        try:
            LOOP.call_soon_threadsafe(LOOP.stop)
        except Exception:
            pass


if __name__ == "__main__":
    main()
