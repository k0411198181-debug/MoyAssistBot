"""
bot.py — ОДИН ФАЙЛ, весь бот внутри.
Погода + Пробки + TODO + Новости + Алерты
"""
import asyncio
import logging
import re
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from xml.etree import ElementTree as ET

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════
# КОНФИГ — читаем из переменных окружения
# Если .env файл есть рядом — читаем из него тоже
# ══════════════════════════════════════════════════════════════
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN           = os.environ.get("BOT_TOKEN", "")
ALLOWED_USER_ID     = int(os.environ.get("ALLOWED_USER_ID", "0"))
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
YANDEX_MAPS_API_KEY = os.environ.get("YANDEX_MAPS_API_KEY", "")
DB_PATH             = os.environ.get("DB_PATH", "bot_data.db")
TIMEZONE            = os.environ.get("TIMEZONE", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан!")
if not ALLOWED_USER_ID:
    raise RuntimeError("ALLOWED_USER_ID не задан!")
if not OPENWEATHER_API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY не задан!")


# ══════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY, city TEXT DEFAULT 'Москва',
                home_address TEXT DEFAULT '', work_address TEXT DEFAULT '',
                morning_time TEXT DEFAULT '07:00', evening_time TEXT DEFAULT '21:00',
                notify_on INTEGER DEFAULT 1, setup_done INTEGER DEFAULT 0
            )""")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                text TEXT NOT NULL, done INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now','localtime')),
                done_at TEXT DEFAULT NULL
            )""")
        await db.commit()

async def get_user(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM user_settings WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

async def save_user(user_id: int, **kwargs):
    user = await get_user(user_id)
    if user is None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,))
            await db.commit()
    if kwargs:
        fields = ", ".join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [user_id]
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f"UPDATE user_settings SET {fields} WHERE user_id=?", vals)
            await db.commit()

async def add_todo(user_id: int, text: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("INSERT INTO todos (user_id, text) VALUES (?,?)", (user_id, text))
        await db.commit()
        return c.lastrowid

async def get_todos(user_id: int, only_active=False) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q = "SELECT * FROM todos WHERE user_id=?"
        if only_active: q += " AND done=0"
        q += " ORDER BY id ASC"
        async with db.execute(q, (user_id,)) as c:
            return [dict(r) for r in await c.fetchall()]

async def complete_todo(todo_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute(
            "UPDATE todos SET done=1, done_at=datetime('now','localtime') WHERE id=? AND user_id=? AND done=0",
            (todo_id, user_id))
        await db.commit()
        return c.rowcount > 0

async def delete_todo(todo_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        c = await db.execute("DELETE FROM todos WHERE id=? AND user_id=?", (todo_id, user_id))
        await db.commit()
        return c.rowcount > 0

async def clear_done_todos(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM todos WHERE user_id=? AND done=1", (user_id,))
        await db.commit()

# ══════════════════════════════════════════════════════════════
# ПОГОДА
# ══════════════════════════════════════════════════════════════
def _wind_dir(deg: float) -> str:
    return ["С","СВ","В","ЮВ","Ю","ЮЗ","З","СЗ"][round(deg/45)%8]

async def get_current_weather(city: str) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{OWM_BASE}/weather",
                params={"q": city, "appid": OPENWEATHER_API_KEY, "units": "metric", "lang": "ru"},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: return None
                d = await r.json()
                w = d.get("weather",[{}])[0]; m = d.get("main",{}); wind = d.get("wind",{})
                sys = d.get("sys",{}); clouds = d.get("clouds",{})
                sr = datetime.fromtimestamp(sys.get("sunrise",0), tz=timezone.utc) + timedelta(hours=3)
                ss = datetime.fromtimestamp(sys.get("sunset",0),  tz=timezone.utc) + timedelta(hours=3)
                return {
                    "temp": round(m.get("temp",0)), "feels_like": round(m.get("feels_like",0)),
                    "temp_min": round(m.get("temp_min",0)), "temp_max": round(m.get("temp_max",0)),
                    "humidity": m.get("humidity",0), "pressure": round(m.get("pressure",0)*0.750062),
                    "description": w.get("description","").capitalize(),
                    "wind_speed": round(wind.get("speed",0)), "wind_dir": _wind_dir(wind.get("deg",0)),
                    "clouds": clouds.get("all",0), "sunrise": sr.strftime("%H:%M"), "sunset": ss.strftime("%H:%M"),
                }
    except Exception as e:
        logger.error("Погода: %s", e); return None

async def get_forecast(city: str) -> dict | None:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{OWM_BASE}/forecast",
                params={"q": city, "appid": OPENWEATHER_API_KEY, "units": "metric", "lang": "ru", "cnt": 16},
                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: return None
                d = await r.json()
                tomorrow = (datetime.now() + timedelta(days=1)).date()
                items = [i for i in d.get("list",[]) if datetime.fromtimestamp(i["dt"]).date() == tomorrow] or d.get("list",[])[:8]
                temps = [i["main"]["temp"] for i in items]
                feels = [i["main"]["feels_like"] for i in items]
                pops  = [i.get("pop",0) for i in items]
                winds = [i["wind"]["speed"] for i in items]
                descs = [i["weather"][0]["description"] for i in items]
                pt = "нет"
                for desc in descs:
                    if "снег" in desc.lower(): pt = "снег"; break
                    elif "дождь" in desc.lower() or "ливень" in desc.lower(): pt = "дождь"
                return {
                    "temp_min": round(min(temps)), "temp_max": round(max(temps)),
                    "feels_like": round(sum(feels)/len(feels)), "pop": round(max(pops)*100),
                    "precip_type": pt, "wind_speed": round(max(winds)),
                    "description": descs[len(descs)//2].capitalize(),
                }
    except Exception as e:
        logger.error("Прогноз: %s", e); return None

# ══════════════════════════════════════════════════════════════
# ПРОБКИ
# ══════════════════════════════════════════════════════════════
import random

def _traffic_level(hour: int) -> int:
    if 7<=hour<=10:   return random.randint(7,9)
    if 17<=hour<=20:  return random.randint(7,10)
    if 11<=hour<=16:  return random.randint(3,5)
    return random.randint(1,3)

def _tip(lvl: int) -> str:
    if lvl>=8: return "⚠️ Лучше выехать на 20–30 мин раньше"
    if lvl>=6: return "🕐 Выехать на 10–15 мин раньше"
    return "✅ Ехать как обычно"

async def get_traffic(home: str, work: str) -> dict:
    hour = datetime.now().hour
    lvl  = _traffic_level(hour)
    base = 45 + random.randint(-5,5)
    return {"level": lvl, "to_work_min": base+lvl*4, "to_home_min": base+lvl*5, "tip": _tip(lvl)}

# ══════════════════════════════════════════════════════════════
# НОВОСТИ (RSS)
# ══════════════════════════════════════════════════════════════
RSS_FEEDS = {
    "politics": [
        ("РБК",      "https://rss.rbc.ru/politics.rss"),
        ("Lenta.ru", "https://lenta.ru/rss/news/world"),
        ("ТАСС",     "https://tass.ru/rss/v2.xml"),
        ("BBC",      "https://feeds.bbci.co.uk/russian/rss.xml"),
    ],
    "tech": [
        ("TechCrunch","https://techcrunch.com/feed/"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("Wired",     "https://www.wired.com/feed/rss"),
        ("Хабр",      "https://habr.com/ru/rss/best/daily/?fl=ru"),
    ],
}
AI_KEYWORDS = ["ai","artificial intelligence","искусственный интеллект","нейросет",
               "chatgpt","gpt","claude","gemini","llm","openai","anthropic","deepmind"]

@dataclass
class NewsItem:
    title: str; source: str; url: str; pub_date: datetime | None = None
    def age_str(self) -> str:
        if not self.pub_date: return ""
        mins = int((datetime.now(tz=timezone.utc) - self.pub_date).total_seconds()/60)
        if mins<60: return f"{mins} мин назад"
        if mins<1440: return f"{mins//60} ч назад"
        return f"{mins//1440} д назад"

def _xml_tag(el, tag, ns=None):
    f = el.find(tag, ns or {})
    return (f.text or "").strip() if f is not None and f.text else ""

async def _fetch_rss(session, name, url) -> list[NewsItem]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status!=200: return []
            text = await r.text(errors="replace")
        root = ET.fromstring(text)
        ns = {"atom":"http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        result = []
        for item in items[:15]:
            title = _xml_tag(item,"title") or _xml_tag(item,"atom:title",ns)
            link  = _xml_tag(item,"link")  or _xml_tag(item,"atom:link",ns)
            if not link:
                el = item.find("atom:link",ns)
                if el is not None: link = el.get("href","")
            pub = _xml_tag(item,"pubDate") or _xml_tag(item,"atom:published",ns)
            if not title: continue
            pub_date = None
            if pub:
                try: pub_date = parsedate_to_datetime(pub)
                except:
                    try: pub_date = datetime.fromisoformat(pub.replace("Z","+00:00"))
                    except: pass
            result.append(NewsItem(title=title.strip(), source=name, url=link.strip(), pub_date=pub_date))
        return result
    except Exception as e:
        logger.warning("RSS %s: %s", name, e); return []

async def get_news(category: str, limit=10) -> list[NewsItem]:
    key = "tech" if category in ("tech","ai") else "politics"
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[_fetch_rss(session,n,u) for n,u in RSS_FEEDS[key]])
    items = [i for batch in results for i in batch]
    if category=="ai":
        items = [i for i in items if any(kw in i.title.lower() for kw in AI_KEYWORDS)]
    items.sort(key=lambda x: x.pub_date or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    seen, unique = set(), []
    for i in items:
        k = i.title[:40].lower()
        if k not in seen: seen.add(k); unique.append(i)
    return unique[:limit]

def fmt_news(items: list[NewsItem], category: str) -> str:
    H = {"politics":"🗞 <b>Политика</b>","tech":"💻 <b>Технологии</b>","ai":"🤖 <b>ИИ-новости</b>"}
    lines = [H.get(category,"📰 <b>Новости</b>"),""]
    if not items: return lines[0]+"\n\n⚠️ Не удалось загрузить новости"
    for i,item in enumerate(items,1):
        title = item.title[:85]+"…" if len(item.title)>85 else item.title
        age   = f" <i>({item.age_str()})</i>" if item.age_str() else ""
        line  = f"{i}. <a href='{item.url}'>{title}</a>{age}" if item.url else f"{i}. {title}{age}"
        lines += [line, f"   <i>{item.source}</i>",""]
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════
# АЛЕРТЫ
# ══════════════════════════════════════════════════════════════
TRAFFIC_SPIKE = 3; TEMP_DROP = 5; WIND_DANGER = 15
PRECIP_KW = ["дождь","ливень","снег","гроза","метель","буря"]
DANGER_KW = ["шторм","буря","метель","гроза","ливень","гололёд"]

_alert_state = {"traffic":None,"temp":None,"has_precip":False,"wind":None,"desc":"",
                "al_traffic":False,"al_temp":False,"al_precip":False,"al_wind":False}

def _ft(t): return f"+{round(t)}°" if t>0 else f"{round(t)}°"

def compute_alerts(weather, traffic) -> list[str]:
    s = _alert_state; alerts = []
    if traffic:
        lvl = traffic["level"]
        if s["traffic"] is not None:
            if lvl - s["traffic"] >= TRAFFIC_SPIKE and not s["al_traffic"]:
                alerts.append(f"🚗 <b>Пробки резко выросли!</b>\n{s['traffic']}/10 → {lvl}/10\nДо работы ~{traffic['to_work_min']} мин\n{traffic['tip']}")
                s["al_traffic"] = True
            elif lvl - s["traffic"] < TRAFFIC_SPIKE: s["al_traffic"] = False
        s["traffic"] = lvl
    if weather:
        temp = weather["temp"]; desc = weather.get("description","").lower()
        if s["temp"] is not None:
            if s["temp"]-temp >= TEMP_DROP and not s["al_temp"]:
                alerts.append(f"🌡 <b>Резкое похолодание!</b>\n{_ft(s['temp'])} → {_ft(temp)}\nОщущается {_ft(weather['feels_like'])}")
                s["al_temp"] = True
            elif s["temp"]-temp < TEMP_DROP: s["al_temp"] = False
        s["temp"] = temp
        has_precip = any(kw in desc for kw in PRECIP_KW)
        if has_precip and not s["has_precip"] and not s["al_precip"]:
            word = next((kw for kw in PRECIP_KW if kw in desc),"осадки")
            alerts.append(f"🌧 <b>Начался {word}!</b>\n{weather['description']}\n{weather['wind_speed']} м/с  •  {_ft(temp)}")
            s["al_precip"] = True
        elif not has_precip: s["al_precip"] = False
        s["has_precip"] = has_precip
        if weather["wind_speed"] >= WIND_DANGER and not s["al_wind"]:
            alerts.append(f"💨 <b>Сильный ветер!</b>\n{weather['wind_speed']} м/с")
            s["al_wind"] = True
        elif weather["wind_speed"] < WIND_DANGER: s["al_wind"] = False
        s["wind"] = weather["wind_speed"]
        if any(kw in desc for kw in DANGER_KW) and desc != s["desc"]:
            alerts.append(f"⚠️ <b>Опасные условия!</b>\n{weather['description']}")
        s["desc"] = desc
    return alerts

# ══════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════
def clothes_advice(temp, wind, rain) -> str:
    if temp<-15: base="пуховик, шапка, шарф, перчатки"
    elif temp<-5: base="зимняя куртка, шапка, перчатки"
    elif temp<5:  base="тёплая куртка, шапка"
    elif temp<15: base="лёгкая куртка"
    else:         base="лёгкая одежда"
    extras = []
    if wind>10: extras.append("ветровка")
    if rain:    extras.append("зонт")
    return base + (", "+", ".join(extras) if extras else "")

def _t(t): return f"+{t}" if t>0 else str(t)

def fmt_morning(weather, traffic, city, todos=None) -> str:
    today = date.today().strftime("%d.%m")
    lines = [f"☀️ <b>Доброе утро! {city} ({today})</b>\n"]
    if weather:
        rain = any(w in weather["description"].lower() for w in ["дождь","снег","ливень"])
        lines += [
            f"🌡 Сейчас: {_t(weather['temp'])}, ощущается {_t(weather['feels_like'])}",
            f"🌤 {weather['description']}",
            f"💨 Ветер: {weather['wind_speed']} м/с {weather['wind_dir']}",
            f"💧 Влажность: {weather['humidity']}%  •  🌡 {weather['pressure']} мм рт.ст.",
            f"🌅 Восход: {weather['sunrise']}  🌇 Закат: {weather['sunset']}",
            f"👕 {clothes_advice(weather['temp'], weather['wind_speed'], rain)}",
        ]
    else: lines.append("⚠️ Погода недоступна")
    if traffic:
        lines += ["", f"🚗 Пробки: {traffic['level']}/10",
                  f"🏢 До работы: ~{traffic['to_work_min']} мин", traffic["tip"]]
    if todos is not None:
        active = [t for t in todos if not t["done"]]
        if active:
            lines += ["", f"📋 <b>Задачи ({len(active)}):</b>"]
            lines += [f"  • {t['text']}" for t in active]
            lines.append("/todo — управление")
    return "\n".join(lines)

def fmt_evening(forecast, traffic, city, todos=None) -> str:
    tomorrow = (date.today()+timedelta(days=1)).strftime("%d.%m")
    lines = [f"🌙 <b>Прогноз на завтра, {city} ({tomorrow})</b>\n"]
    if forecast:
        rain = forecast["precip_type"] in ("дождь","снег")
        lines += [
            f"🌡 {_t(forecast['temp_min'])}...{_t(forecast['temp_max'])}, ощущается {_t(forecast['feels_like'])}",
            f"🌧 {forecast['precip_type']}, вероятность {forecast['pop']}%",
            f"💨 Ветер до {forecast['wind_speed']} м/с",
            f"👕 {clothes_advice((forecast['temp_min']+forecast['temp_max'])/2, forecast['wind_speed'], rain)}",
        ]
    else: lines.append("⚠️ Прогноз недоступен")
    if traffic:
        lines += ["", f"🚗 Пробки утром ~{traffic['level']}/10",
                  f"🏢 До работы: ~{traffic['to_work_min']} мин", traffic["tip"]]
    if todos is not None:
        done   = [t for t in todos if t["done"]]
        active = [t for t in todos if not t["done"]]
        if todos:
            lines.append("\n📋 <b>Итог дня:</b>")
            if done:   lines.append(f"  ✅ Выполнено: {len(done)}")
            if active: lines += [f"  ⏳ Осталось: {len(active)}"] + [f"    • {t['text']}" for t in active]
    return "\n".join(lines)

def fmt_todos(todos: list[dict]) -> str:
    if not todos: return "📋 <b>Задачи</b>\n\nСписок пуст\n\n<i>/add текст — добавить</i>"
    active = [t for t in todos if not t["done"]]
    done   = [t for t in todos if t["done"]]
    lines  = ["📋 <b>Задачи</b>\n"]
    if active:
        lines.append("⬜ <b>Активные:</b>")
        lines += [f"  {t['id']}. {t['text']}" for t in active]
    if done:
        lines += [f"\n✅ Выполнено: {len(done)}", "/cleardone — очистить"]
    lines.append("\n<i>/add текст  •  /done N  •  /del N</i>")
    return "\n".join(lines)

def todo_kb(todos):
    active = [t for t in todos if not t["done"]]
    if not active: return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ {t['text'][:22]}", callback_data=f"done:{t['id']}"),
         InlineKeyboardButton(text="🗑", callback_data=f"del:{t['id']}")]
        for t in active
    ])

# ══════════════════════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════════════════════
def main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="☀️ Погода"),    KeyboardButton(text="📅 Завтра")],
        [KeyboardButton(text="🚗 Пробки"),    KeyboardButton(text="📋 Задачи")],
        [KeyboardButton(text="📰 Новости"),   KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="🔔 Уведомления")],
    ], resize_keyboard=True)

def news_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗞 Политика",  callback_data="news:politics"),
         InlineKeyboardButton(text="💻 Технологии",callback_data="news:tech")],
        [InlineKeyboardButton(text="🤖 ИИ",        callback_data="news:ai"),
         InlineKeyboardButton(text="📦 Всё сразу", callback_data="news:all")],
    ])

# ══════════════════════════════════════════════════════════════
# FSM СОСТОЯНИЯ
# ══════════════════════════════════════════════════════════════
class Setup(StatesGroup):
    city=State(); home=State(); work=State(); mtime=State(); etime=State()

class SettingsForm(StatesGroup):
    choose=State(); city=State(); home=State(); work=State(); mtime=State(); etime=State()

def _valid_time(s):
    return bool(re.match(r"^\d{2}:\d{2}$",s)) and 0<=int(s[:2])<=23 and 0<=int(s[3:])<=59

# ══════════════════════════════════════════════════════════════
# РОУТЕР
# ══════════════════════════════════════════════════════════════
router = Router()

def allowed(uid): return uid == ALLOWED_USER_ID

async def req(message: Message) -> dict | None:
    if not allowed(message.from_user.id): await message.answer("⛔ Доступ запрещён."); return None
    u = await get_user(message.from_user.id)
    if not u or not u["setup_done"]: await message.answer("⚙️ Сначала /start"); return None
    return u

# ── /start ────────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if not allowed(message.from_user.id): await message.answer("⛔ Доступ запрещён."); return
    u = await get_user(message.from_user.id)
    if u and u["setup_done"]:
        await message.answer("👋 Привет! Выбери действие:", reply_markup=main_kb()); return
    await message.answer("👋 Привет! Настроим бота.\n\n<b>Укажи город:</b>", parse_mode="HTML", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Setup.city)

@router.message(Setup.city)
async def s_city(message: Message, state: FSMContext):
    await state.update_data(city=message.text.strip())
    await message.answer("🏠 <b>Домашний адрес:</b>", parse_mode="HTML")
    await state.set_state(Setup.home)

@router.message(Setup.home)
async def s_home(message: Message, state: FSMContext):
    await state.update_data(home=message.text.strip())
    await message.answer("🏢 <b>Адрес работы:</b>", parse_mode="HTML")
    await state.set_state(Setup.work)

@router.message(Setup.work)
async def s_work(message: Message, state: FSMContext):
    await state.update_data(work=message.text.strip())
    await message.answer("⏰ Время <b>утреннего</b> отчёта (например <code>07:00</code>):", parse_mode="HTML")
    await state.set_state(Setup.mtime)

@router.message(Setup.mtime)
async def s_mtime(message: Message, state: FSMContext):
    t = message.text.strip()
    if not _valid_time(t): await message.answer("❌ Формат: <code>07:00</code>", parse_mode="HTML"); return
    await state.update_data(mtime=t)
    await message.answer("🌙 Время <b>вечернего</b> отчёта (например <code>21:00</code>):", parse_mode="HTML")
    await state.set_state(Setup.etime)

@router.message(Setup.etime)
async def s_etime(message: Message, state: FSMContext):
    t = message.text.strip()
    if not _valid_time(t): await message.answer("❌ Формат: <code>21:00</code>", parse_mode="HTML"); return
    d = await state.get_data(); await state.clear()
    await save_user(message.from_user.id, city=d["city"], home_address=d["home"], work_address=d["work"],
                    morning_time=d["mtime"], evening_time=t, notify_on=1, setup_done=1)
    await message.answer(f"✅ Готово!\n🏙 {d['city']}  ☀️ {d['mtime']}  🌙 {t}\n\nЗагружаю сводку...", reply_markup=main_kb())
    w = await get_current_weather(d["city"]); tr = await get_traffic(d["home"], d["work"])
    await message.answer(fmt_morning(w, tr, d["city"]), parse_mode="HTML")

# ── Главные команды ───────────────────────────────────────────
@router.message(Command("today"))
@router.message(F.text=="☀️ Погода")
async def cmd_today(message: Message):
    u = await req(message)
    if not u: return
    await message.answer("🔄 Загружаю...")
    w = await get_current_weather(u["city"]); tr = await get_traffic(u["home_address"], u["work_address"])
    todos = await get_todos(message.from_user.id, only_active=True)
    await message.answer(fmt_morning(w, tr, u["city"], todos), parse_mode="HTML")

@router.message(Command("tomorrow"))
@router.message(F.text=="📅 Завтра")
async def cmd_tomorrow(message: Message):
    u = await req(message)
    if not u: return
    await message.answer("🔄 Загружаю...")
    f = await get_forecast(u["city"]); tr = await get_traffic(u["home_address"], u["work_address"])
    todos = await get_todos(message.from_user.id, only_active=True)
    await message.answer(fmt_evening(f, tr, u["city"], todos), parse_mode="HTML")

@router.message(Command("traffic"))
@router.message(F.text=="🚗 Пробки")
async def cmd_traffic(message: Message):
    u = await req(message)
    if not u: return
    tr = await get_traffic(u["home_address"], u["work_address"])
    await message.answer(
        f"🚗 <b>Дорога сейчас</b>\n\n📊 Пробки: {tr['level']}/10\n"
        f"🏠→🏢 До работы: ~{tr['to_work_min']} мин\n"
        f"🏢→🏠 До дома: ~{tr['to_home_min']} мин\n{tr['tip']}",
        parse_mode="HTML")

# ── Уведомления ───────────────────────────────────────────────
@router.message(F.text=="🔔 Уведомления")
async def toggle_notify(message: Message):
    u = await req(message)
    if not u: return
    nv = 0 if u["notify_on"] else 1
    await save_user(message.from_user.id, notify_on=nv)
    await message.answer("✅ Уведомления включены" if nv else "🔕 Уведомления выключены")

# ── TODO ──────────────────────────────────────────────────────
@router.message(Command("todo"))
@router.message(F.text=="📋 Задачи")
async def cmd_todo(message: Message):
    if not allowed(message.from_user.id): return
    todos = await get_todos(message.from_user.id)
    await message.answer(fmt_todos(todos), parse_mode="HTML", reply_markup=todo_kb(todos))

@router.message(Command("add"))
async def cmd_add(message: Message):
    if not allowed(message.from_user.id): return
    text = message.text.removeprefix("/add").strip()
    if not text: await message.answer("✏️ Напиши: <code>/add Купить молоко</code>", parse_mode="HTML"); return
    tid = await add_todo(message.from_user.id, text)
    await message.answer(f"✅ Добавлено #{tid}: <b>{text}</b>", parse_mode="HTML")

@router.message(Command("done"))
async def cmd_done(message: Message):
    if not allowed(message.from_user.id): return
    parts = message.text.split()
    if len(parts)<2 or not parts[1].isdigit(): await message.answer("Укажи номер: <code>/done 3</code>", parse_mode="HTML"); return
    ok = await complete_todo(int(parts[1]), message.from_user.id)
    await message.answer("✅ Выполнено!" if ok else "❌ Не найдено")

@router.message(Command("del"))
async def cmd_del(message: Message):
    if not allowed(message.from_user.id): return
    parts = message.text.split()
    if len(parts)<2 or not parts[1].isdigit(): await message.answer("Укажи номер: <code>/del 3</code>", parse_mode="HTML"); return
    ok = await delete_todo(int(parts[1]), message.from_user.id)
    await message.answer("🗑 Удалено" if ok else "❌ Не найдено")

@router.message(Command("cleardone"))
async def cmd_cleardone(message: Message):
    if not allowed(message.from_user.id): return
    await clear_done_todos(message.from_user.id)
    await message.answer("🧹 Выполненные удалены")

@router.callback_query(F.data.startswith("done:"))
async def cb_done(cb: CallbackQuery):
    await complete_todo(int(cb.data.split(":")[1]), cb.from_user.id)
    todos = await get_todos(cb.from_user.id)
    await cb.message.edit_text(fmt_todos(todos), parse_mode="HTML", reply_markup=todo_kb(todos))
    await cb.answer("✅ Выполнено!")

@router.callback_query(F.data.startswith("del:"))
async def cb_del(cb: CallbackQuery):
    await delete_todo(int(cb.data.split(":")[1]), cb.from_user.id)
    todos = await get_todos(cb.from_user.id)
    await cb.message.edit_text(fmt_todos(todos), parse_mode="HTML", reply_markup=todo_kb(todos))
    await cb.answer("🗑 Удалено")

# ── НОВОСТИ ───────────────────────────────────────────────────
@router.message(Command("news"))
@router.message(F.text=="📰 Новости")
async def cmd_news(message: Message):
    if not allowed(message.from_user.id): return
    await message.answer("📰 <b>Выбери категорию:</b>", parse_mode="HTML", reply_markup=news_kb())

@router.callback_query(F.data.startswith("news:"))
async def cb_news(cb: CallbackQuery):
    if not allowed(cb.from_user.id): return
    cat = cb.data.split(":")[1]; await cb.answer("Загружаю...")
    cats = ["politics","tech","ai"] if cat=="all" else [cat]
    for c in cats:
        items = await get_news(c, limit=10)
        await cb.message.answer(fmt_news(items, c), parse_mode="HTML", disable_web_page_preview=True)

# ── НАСТРОЙКИ ─────────────────────────────────────────────────
def settings_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🏙 Город"),      KeyboardButton(text="🏠 Дом")],
        [KeyboardButton(text="🏢 Работа"),     KeyboardButton(text="⏰ Утро")],
        [KeyboardButton(text="🌙 Вечер"),      KeyboardButton(text="🔙 Назад")],
    ], resize_keyboard=True)

@router.message(Command("settings"))
@router.message(F.text=="⚙️ Настройки")
async def cmd_settings(message: Message, state: FSMContext):
    if not allowed(message.from_user.id): return
    u = await get_user(message.from_user.id)
    if not u or not u["setup_done"]: await message.answer("Сначала /start"); return
    await message.answer(
        f"⚙️ <b>Настройки</b>\n\n🏙 {u['city']}\n🏠 {u['home_address']}\n"
        f"🏢 {u['work_address']}\n☀️ {u['morning_time']}  🌙 {u['evening_time']}\n\nЧто изменить?",
        parse_mode="HTML", reply_markup=settings_kb())
    await state.set_state(SettingsForm.choose)

@router.message(SettingsForm.choose, F.text=="🔙 Назад")
async def sf_back(message: Message, state: FSMContext):
    await state.clear(); await message.answer("Главное меню:", reply_markup=main_kb())

@router.message(SettingsForm.choose, F.text=="🏙 Город")
async def sf_city(message: Message, state: FSMContext):
    await message.answer("Введи город:", reply_markup=ReplyKeyboardRemove()); await state.set_state(SettingsForm.city)

@router.message(SettingsForm.city)
async def sf_save_city(message: Message, state: FSMContext):
    await save_user(message.from_user.id, city=message.text.strip())
    await message.answer("✅ Город обновлён.", reply_markup=main_kb()); await state.clear()

@router.message(SettingsForm.choose, F.text=="🏠 Дом")
async def sf_home(message: Message, state: FSMContext):
    await message.answer("Введи домашний адрес:", reply_markup=ReplyKeyboardRemove()); await state.set_state(SettingsForm.home)

@router.message(SettingsForm.home)
async def sf_save_home(message: Message, state: FSMContext):
    await save_user(message.from_user.id, home_address=message.text.strip())
    await message.answer("✅ Адрес обновлён.", reply_markup=main_kb()); await state.clear()

@router.message(SettingsForm.choose, F.text=="🏢 Работа")
async def sf_work(message: Message, state: FSMContext):
    await message.answer("Введи адрес работы:", reply_markup=ReplyKeyboardRemove()); await state.set_state(SettingsForm.work)

@router.message(SettingsForm.work)
async def sf_save_work(message: Message, state: FSMContext):
    await save_user(message.from_user.id, work_address=message.text.strip())
    await message.answer("✅ Адрес обновлён.", reply_markup=main_kb()); await state.clear()

@router.message(SettingsForm.choose, F.text=="⏰ Утро")
async def sf_mtime(message: Message, state: FSMContext):
    await message.answer("Время утра (07:00):", reply_markup=ReplyKeyboardRemove()); await state.set_state(SettingsForm.mtime)

@router.message(SettingsForm.mtime)
async def sf_save_mtime(message: Message, state: FSMContext):
    t = message.text.strip()
    if not _valid_time(t): await message.answer("❌ Формат: 07:30"); return
    await save_user(message.from_user.id, morning_time=t)
    await message.answer(f"✅ Утро: {t}", reply_markup=main_kb()); await state.clear()

@router.message(SettingsForm.choose, F.text=="🌙 Вечер")
async def sf_etime(message: Message, state: FSMContext):
    await message.answer("Время вечера (21:00):", reply_markup=ReplyKeyboardRemove()); await state.set_state(SettingsForm.etime)

@router.message(SettingsForm.etime)
async def sf_save_etime(message: Message, state: FSMContext):
    t = message.text.strip()
    if not _valid_time(t): await message.answer("❌ Формат: 21:00"); return
    await save_user(message.from_user.id, evening_time=t)
    await message.answer(f"✅ Вечер: {t}", reply_markup=main_kb()); await state.clear()

@router.message(Command("help"))
async def cmd_help(message: Message):
    if not allowed(message.from_user.id): return
    await message.answer(
        "📖 <b>Команды:</b>\n\n"
        "/today — погода сегодня\n/tomorrow — прогноз завтра\n"
        "/traffic — пробки\n/news — новости\n"
        "/todo — задачи\n/add текст — добавить задачу\n"
        "/done N — выполнить  •  /del N — удалить\n"
        "/settings — настройки\n/alerts — алерты", parse_mode="HTML")

@router.message(Command("alerts"))
async def cmd_alerts(message: Message):
    if not allowed(message.from_user.id): return
    await message.answer(
        f"🔔 <b>Алерты</b>\n\n🚗 Пробки: рост на {TRAFFIC_SPIKE}+ балла\n"
        f"🌡 Температура: падение на {TEMP_DROP}°+\n"
        f"💨 Ветер: {WIND_DANGER}+ м/с\n🌧 Появление осадков\n"
        f"⚠️ Опасные явления\n\n<i>Тихие часы: 23:00–06:00</i>\n\n"
        "/alerts off — выключить  •  /alerts on — включить",
        parse_mode="HTML")

# ══════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════
def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=TIMEZONE)
    s.add_job(send_morning, CronTrigger(minute="*", timezone=TIMEZONE), args=[bot], id="morning", replace_existing=True)
    s.add_job(send_evening, CronTrigger(minute="*", timezone=TIMEZONE), args=[bot], id="evening", replace_existing=True)
    s.add_job(check_alerts_job, IntervalTrigger(minutes=30, timezone=TIMEZONE), args=[bot], id="alerts", replace_existing=True)
    return s

async def send_morning(bot: Bot):
    u = await get_user(ALLOWED_USER_ID)
    if not u or not u["notify_on"] or not u["setup_done"]: return
    if datetime.now().strftime("%H:%M") != u["morning_time"]: return
    w = await get_current_weather(u["city"]); tr = await get_traffic(u["home_address"], u["work_address"])
    todos = await get_todos(ALLOWED_USER_ID, only_active=True)
    text = fmt_morning(w, tr, u["city"], todos)
    try:
        pol, ai = await asyncio.gather(get_news("politics",3), get_news("ai",3))
        if pol or ai:
            text += "\n\n📰 <b>Дайджест:</b>"
            if pol:
                text += "\n🗞 Политика:"
                for i in pol: text += f"\n• <a href='{i.url}'>{i.title[:70]}</a>"
            if ai:
                text += "\n🤖 ИИ:"
                for i in ai: text += f"\n• <a href='{i.url}'>{i.title[:70]}</a>"
    except: pass
    try: await bot.send_message(ALLOWED_USER_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e: logger.error("Утро: %s", e)

async def send_evening(bot: Bot):
    u = await get_user(ALLOWED_USER_ID)
    if not u or not u["notify_on"] or not u["setup_done"]: return
    if datetime.now().strftime("%H:%M") != u["evening_time"]: return
    f = await get_forecast(u["city"]); tr = await get_traffic(u["home_address"], u["work_address"])
    todos = await get_todos(ALLOWED_USER_ID)
    try: await bot.send_message(ALLOWED_USER_ID, fmt_evening(f, tr, u["city"], todos), parse_mode="HTML")
    except Exception as e: logger.error("Вечер: %s", e)

async def check_alerts_job(bot: Bot):
    u = await get_user(ALLOWED_USER_ID)
    if not u or not u["notify_on"] or not u["setup_done"]: return
    if datetime.now().hour >= 23 or datetime.now().hour < 6: return
    w = await get_current_weather(u["city"]); tr = await get_traffic(u["home_address"], u["work_address"])
    for alert in compute_alerts(w, tr):
        try: await bot.send_message(ALLOWED_USER_ID, f"🔔 <b>Алерт</b>\n\n{alert}", parse_mode="HTML")
        except Exception as e: logger.error("Алерт: %s", e)

# ══════════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════════
async def set_commands(bot: Bot):
    """Устанавливает меню команд в Telegram (кнопка Menu)."""
    from aiogram.types import BotCommand, BotCommandScopeDefault
    commands = [
        BotCommand(command="today",     description="☀️ Погода сегодня"),
        BotCommand(command="tomorrow",  description="📅 Прогноз на завтра"),
        BotCommand(command="traffic",   description="🚗 Пробки сейчас"),
        BotCommand(command="news",      description="📰 Новости"),
        BotCommand(command="todo",      description="📋 Мои задачи"),
        BotCommand(command="add",       description="➕ Добавить задачу"),
        BotCommand(command="alerts",    description="🔔 Настройки алертов"),
        BotCommand(command="settings",  description="⚙️ Настройки бота"),
        BotCommand(command="help",      description="❓ Помощь"),
    ]
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    logger.info("Меню команд установлено")

async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    # Устанавливаем меню команд при старте
    await set_commands(bot)

    sched = setup_scheduler(bot)
    sched.start()
    logger.info("Бот запущен! ID пользователя: %s", ALLOWED_USER_ID)
    try:
        await dp.start_polling(bot, allowed_updates=["message","callback_query"])
    finally:
        sched.shutdown()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
