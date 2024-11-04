import asyncio
import json
import logging
import qrcode
import random
import re
import aiosqlite
import string
import datetime
from datetime import datetime as dt
from io import BytesIO
from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils import exceptions
from aiogram.types import ParseMode, ContentType
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import exceptions as aiogram_exceptions
from aiogram.dispatcher import FSMContext
from aiogram.utils.callback_data import CallbackData
from logging.handlers import RotatingFileHandler
import aiohttp
import os
import time
from dotenv import load_dotenv
import sys

load_dotenv()

# Конфигурационные параметры
PAYMENTS_TOKEN = os.getenv("PAYMENTS_TOKEN")
BOT_TOKEN = os.getenv("BOT_TOKEN")
LOGIN_URL = os.getenv("LOGIN_URL")
LOGIN_DATA = {"username": os.getenv("LOGIN_USERNAME"), "password": os.getenv("LOGIN_PASSWORD")}
SERVER_IP = os.getenv("SERVER_IP")
SERVER_URL = os.getenv("SERVER_URL")
DATABASE = os.getenv("DATABASE_PATH")
USERSDATABASE = os.getenv("USERSDATABASE_PATH")
ADD_CLIENT_URL = os.getenv("ADD_CLIENT_URL")
ID_1 = os.getenv("ID_1")
ID_2 = os.getenv("ID_2")
ID_3 = os.getenv("ID_3")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = [int(id.strip()) for id in os.getenv("ADMIN_ID", "").split(',')] if os.getenv("ADMIN_ID") else []

# Цены и тарифы
PRICES = [
    types.LabeledPrice(label="Подписка на 1 месяц", amount=int(os.getenv("PRICE_1_MONTH"))),
    types.LabeledPrice(label="Подписка на 2 месяца", amount=int(os.getenv("PRICE_2_MONTHS"))),
    types.LabeledPrice(label="Подписка на 6 месяцев", amount=int(os.getenv("PRICE_6_MONTHS"))),
    types.LabeledPrice(label="Подписка на 1 год", amount=int(os.getenv("PRICE_1_YEAR")))
]

CHANNEL_LINK = os.getenv("CHANNEL_LINK")
NIK = os.getenv("NIK")

# Настройка бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Настройка логирования с ротацией файлов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = RotatingFileHandler('bot.log', maxBytes=1000000, backupCount=5)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
bot_logger = logging.getLogger('bot')
bot_logger.addHandler(file_handler)

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_cancel = State()

async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except exceptions.BadRequest:
        return False

def subscription_required(func):
    async def wrapper(update, *args, **kwargs):
        if isinstance(update, types.Message):
            user_id = update.from_user.id
            message = update
        elif isinstance(update, types.CallbackQuery):
            user_id = update.from_user.id
            message = update.message
        else:
            return await func(update, *args, **kwargs)

        if await check_subscription(user_id):
            return await func(update, *args, **kwargs)
        else:
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK))
            keyboard.add(types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription"))
            
            message_text = (
                "╔═══ 🔒 ДОСТУП ОГРАНИЧЕН 🔒 ═══╗\n\n"
                "📱 <b>Для доступа к VPN боту</b>\n"
                f"└─ Подпишитесь на канал {CHANNEL_ID}\n\n"
                "🌟 <b>ПРЕИМУЩЕСТВА ПОДПИСКИ:</b>\n"
                "├─ 📢 Актуальные новости и обновления\n"
                "├─ 💎 Эксклюзивные предложения\n"
                "├─ 🛡️ Советы по безопасности\n"
                "├─ 🎁 Специальные акции\n"
                "└─ ⚡️ Быстрый доступ к обновлениям\n\n"
                "╔═══ ⚠️ ВАЖНО ⚠️ ═══╗\n"
                "├─ Подписка обязательна\n"
                "└─ Это займет 5 секунд\n\n"
                "👇 <b>Нажмите на кнопку ниже:</b>"
            )
            
            if isinstance(update, types.CallbackQuery):
                await update.answer(
                    f"⚠️ Требуется подписка на {CHANNEL_ID} для продолжения",
                    show_alert=True
                )
                await update.message.answer(message_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            else:
                await message.answer(message_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
            return False
    return wrapper

@dp.callback_query_handler(lambda c: c.data == 'check_subscription')
async def check_sub(callback_query: types.CallbackQuery):
    if await check_subscription(callback_query.from_user.id):
        await bot.answer_callback_query(callback_query.id, "Спасибо за подписку! Теперь вы можете использовать бота.")
        await main_menu(callback_query.message)
    else:
        await bot.answer_callback_query(callback_query.id, "Вы еще не подписались на канал. Пожалуйста, подпишитесь для использования бота.", show_alert=True)

async def initialize_database():
    async with aiosqlite.connect(USERSDATABASE) as db:
        # Создаем таблицу с нужными столбцами
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                email TEXT,
                referrer_id INTEGER,
                referral_count INTEGER DEFAULT 0,
                free_configs_count INTEGER DEFAULT 0,
                total_referrals INTEGER DEFAULT 0,
                first_name TEXT,
                username TEXT,
                registration_date TEXT
            )
        ''')
        
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [column[1] for column in columns]
        
        # Добавляем недостающие столбцы
        if 'referral_count' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0')
        
        if 'free_configs_count' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN free_configs_count INTEGER DEFAULT 0')
        
        if 'total_referrals' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN total_referrals INTEGER DEFAULT 0')
            
        if 'first_name' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN first_name TEXT')
            
        if 'username' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN username TEXT')
            
        if 'registration_date' not in column_names:
            await db.execute('ALTER TABLE users ADD COLUMN registration_date TEXT')
            # Обновляем существующие записи текущей датой
            current_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
            await db.execute('UPDATE users SET registration_date = ? WHERE registration_date IS NULL', (current_time,))
        
        await db.commit()
    bot_logger.info("База данных инициализирована")

# Определение состояний бота
class AddClient(StatesGroup):
    WaitingForName = State()
    WaitingForExpiryTime = State()

class GetConfig(StatesGroup):
    EmailInput = State()

class TrialPeriodState(StatesGroup):
    waiting_for_answer = State()

expiry_cb = CallbackData('expiry', 'time')
purchase_cb = CallbackData('purchase', 'action')

async def execute_db_query(db_path, query, params=None):
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(query, params or ()) as cursor:
                if query.strip().upper().startswith("SELECT"):
                    return await cursor.fetchall()
                else:
                    await db.commit()
                    return cursor.rowcount
    except Exception as e:
        bot_logger.error(f"Database error: {e}")
        raise

class Database:
    def __init__(self, db_path):
        self.db_path = db_path

    async def get_stream_settings(self, email):
        query = """
            SELECT
                i.protocol AS prt,
                i.port AS pot,
                json_extract(i.stream_settings, '$.network') AS net,
                json_extract(i.stream_settings, '$.security') AS secur,
                json_extract(i.stream_settings, '$.realitySettings.settings.publicKey') AS pbk,
                json_extract(i.stream_settings, '$.realitySettings.settings.fingerprint') AS fp,
                json_extract(i.stream_settings, '$.realitySettings.serverNames[0]') AS sni,
                json_extract(i.stream_settings, '$.realitySettings.shortIds[0]') AS sid
            FROM
                inbounds i
            JOIN
                client_traffics ct ON i.id = ct.inbound_id
            WHERE
                ct.email = ?;
        """
        return await execute_db_query(self.db_path, query, (email,))

    async def get_ids_by_email(self, email):
        query = """
            SELECT settings
            FROM inbounds
            WHERE settings LIKE '%"email": "{}"%'""".format(email)
        settings = await execute_db_query(self.db_path, query)
        client_ids = []
        for setting in settings:
            setting_data = json.loads(setting[0])
            for client in setting_data['clients']:
                if client['email'] == email:
                    client_ids.append(client['id'])
        return client_ids

    async def get_client_traffics_by_email(self, email):
        query = """
            SELECT email, up, down, expiry_time, total
            FROM client_traffics
            WHERE email = ?"""
        return await execute_db_query(self.db_path, query, (email,))

async def check_server():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(SERVER_URL, ssl=False) as response:
                if response.status == 200:
                    return " ✅ СЕРВЕР АКТИВЕН"
                else:
                    return " ⛔️ СЕРВЕР НЕДОСТУПЕН"
    except aiohttp.ClientError:
        return "⚠️ ОШИБКА СОЕДИНЕНИЯ"

# Функции для работы с клавиатурами
async def get_main_keyboard(user_id=None):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    # Проверяем, использовал ли пользователь пробный период
    if user_id:
        user = await execute_db_query(USERSDATABASE, "SELECT email FROM users WHERE telegram_id=?", (user_id,))
        if not user or not user[0][0]:  # Если пользователь не найден или email пустой
            keyboard.row(types.KeyboardButton(text="🎁 ПРОБНЫЙ ПЕРИОД • 3 ДНЯ"))
    
    keyboard.row(types.KeyboardButton(text="💎 Тарифы"), types.KeyboardButton(text="👤 Профиль"))
    keyboard.row(types.KeyboardButton(text="📚 Инструкция"), types.KeyboardButton(text="🌐 Правила"))
    keyboard.row(types.KeyboardButton(text="🌟 Реферальная система"))
    
    # Проверка на админа с приведением типов
    if user_id and isinstance(ADMIN_ID, list) and user_id in ADMIN_ID:
        keyboard.row(types.KeyboardButton(text="⚙️ АДМИН-ПАНЕЛЬ"))
    
    return keyboard

def get_expiry_time_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="🚀 1 месяц", callback_data=expiry_cb.new(time="-2592000000")))
    keyboard.add(types.InlineKeyboardButton(text="🌟 2 месяца", callback_data=expiry_cb.new(time="-5184000000")))
    keyboard.add(types.InlineKeyboardButton(text="💎 6 месяцев", callback_data=expiry_cb.new(time="-15552000000")))
    keyboard.add(types.InlineKeyboardButton(text="👑 1 год", callback_data=expiry_cb.new(time="-31104000000")))
    keyboard.add(types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_tariffs"))
    return keyboard

def get_purchase_keyboard_with_cancel():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="💳 Купить", callback_data=purchase_cb.new(action="purchase")))
    keyboard.add(types.InlineKeyboardButton(text="🔙 Отмена", callback_data=purchase_cb.new(action="cancel")))
    return keyboard

# Основное меню бота
async def main_menu(message: types.Message, edit: bool = False):
    server_status = await check_server()
    user_name = message.from_user.first_name if message.from_user.first_name else "пользователь"
    welcome_text = (
        "╔══════ 🌟  YUKI VPN 🌟 ══════╗\n\n"
        f"     {server_status}\n\n"
        "✨ <b>ДОСТУПНЫЕ ФУНКЦИИ:</b>\n\n"
        "├─ 💎 Премиум VPN-подключения\n"
        "├─ 🚀 Высокая скорость доступа\n"
        "├─ 🔒 Надёжное шифрование\n"
        "└─ 💫 Бонусная программа\n\n"
        "🛡️ <b>ПРЕИМУЩЕСТВА:</b>\n\n"
        "├─ 📱 Поддержка всех устройств\n"
        "├─ ⚡️ Стабильное соединение\n"
        "├─ 🎯 Без ограничений\n"
        "└─ 🤝 Техподдержка 24/7\n\n"
        "👇 <b>Выберите нужный раздел меню:</b>"
    )

    # Получаем клавиатуру с учетом статуса пробного периода
    keyboard = await get_main_keyboard(message.from_user.id)

    if edit:
        await message.edit_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    else:
        await message.answer(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

# Обновляем функцию admin_panel
@dp.message_handler(lambda message: message.text == "⚙️ АДМИН-ПАНЕЛЬ")
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_ID:
        return
        
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="📢 Создать рассылку", callback_data="start_broadcast"))
    keyboard.add(types.InlineKeyboardButton(text="📊 Статистика бота", callback_data="bot_stats"))
    keyboard.add(types.InlineKeyboardButton(text="👤 Список пользователей", callback_data="users_list"))
    keyboard.add(types.InlineKeyboardButton(text="✉️ Написать пользователю", callback_data="send_to_user"))
    keyboard.add(types.InlineKeyboardButton(text="🔄 Перезагрузить бота", callback_data="restart_bot"))
    
    await message.answer(
        "╔═══ ⚙️ АДМИН-ПАНЕЛЬ ⚙️ ═══╗\n\n"
        "👨‍💻 <b>УПРАВЛЕНИЕ БОТОМ:</b>\n\n"
        "├─ 📢 Рассылка сообщений\n"
        "├─ 📊 Просмотр статистики\n"
        "├─ 👤 Список пользователей\n"
        "├─ ✉️ Отправка сообщений\n"
        "└─ 🔄 Перезагрузка бота",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

# Добавляем обработчик для перезагрузки бота
@dp.callback_query_handler(lambda c: c.data == "restart_bot")
async def restart_bot(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return
    
    try:
        await callback_query.answer("🔄 Бот будет перезагружен...", show_alert=True)
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin"))
        
        await callback_query.message.edit_text(
            "╔═══ 🔄 ПЕРЕЗАГРУЗКА ═══╗\n\n"
            "⏳ <b>Бот перезапускается...</b>\n\n"
            "├─ Сохранение данных\n"
            "├─ Очистка кэша\n"
            "└─ Перезапуск службы\n\n"
            "⚠️ <i>Подождите несколько секунд</i>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        
        # Используем sudo без запроса пароля (требуется настройка sudoers)
        restart_command = ['sudo', 'systemctl', 'restart', 'bot.service']
        
        # Запускаем команду асинхронно
        process = await asyncio.create_subprocess_exec(
            *restart_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Ждем завершения команды
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            # Успешный перезапуск
            await callback_query.message.edit_text(
                "╔═══ ✅ ГОТОВО ═══╗\n\n"
                "<b>Бот успешно перезапущен!</b>\n\n"
                "├─ Служба перезагружена\n"
                "└─ Система готова к работе",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            # Ошибка при перезапуске
            error_msg = stderr.decode() if stderr else "Неизвестная ошибка"
            bot_logger.error(f"Error restarting bot service: {error_msg}")
            raise Exception(error_msg)
            
    except Exception as e:
        bot_logger.error(f"Error in restart_bot: {e}")
        error_keyboard = types.InlineKeyboardMarkup()
        error_keyboard.add(types.InlineKeyboardButton("🔙 Вернуться", callback_data="back_to_admin"))
        
        await callback_query.message.edit_text(
            "╔═══ ❌ ОШИБКА ═══╗\n\n"
            "<b>Не удалось перезапустить бота</b>\n\n"
            "├─ Причина: Внутренняя ошибка\n"
            "└─ Попробуйте позже",
            reply_markup=error_keyboard,
            parse_mode=ParseMode.HTML
        )

# Добавляем новые состояния для отправки сообщения пользователю
class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_message_to_user = State()
    
# Добавляем CallbackData для пагинации
user_info_cb = CallbackData('user_info', 'action', 'id')
send_to_user_cb = CallbackData('send', 'action', 'user_id')
users_page_cb = CallbackData('users', 'action', 'page')

@dp.callback_query_handler(lambda c: c.data == "users_list")
async def show_users_list(callback_query: types.CallbackQuery, page: int = 0):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return
        
    try:
        timestamp = int(time.time())
        total_users = await execute_db_query(USERSDATABASE, "SELECT COUNT(*) FROM users")
        total_users = total_users[0][0] if total_users else 0
        
        # Получаем расширенную информацию о пользователях
        users = await execute_db_query(USERSDATABASE, """
            SELECT telegram_id, email, referrer_id, referral_count, 
                   free_configs_count, total_referrals, first_name, username 
            FROM users 
            ORDER BY telegram_id DESC 
            LIMIT 10 OFFSET ?
        """, (page * 10,))
        
        text = (
            "╔═══ 👥 СПИСОК ПОЛЬЗОВАТЕЛЕЙ 👥 ═══╗\n\n"
            f"📊 Всего пользователей: {total_users}\n"
            f"📄 Страница: {page + 1}/{(total_users + 9) // 10}\n"
            f"🕒 Обновлено: {dt.fromtimestamp(timestamp).strftime('%H:%M:%S')}\n\n"
            "Выберите пользователя для просмотра подробной информации:"
        )

        keyboard = types.InlineKeyboardMarkup(row_width=1)
        
        # Добавляем кнопки для каждого пользователя
        for i, user in enumerate(users, 1):
            telegram_id, email, referrer_id, ref_count, free_conf, total_refs, first_name, username = user
            display_name = first_name or username or f"User {telegram_id}"
            keyboard.add(
                types.InlineKeyboardButton(
                    f"👤 {display_name} | ID: {telegram_id}",
                    callback_data=user_info_cb.new(action='info', id=telegram_id)
                )
            )

        # Кнопки навигации
        nav_row = []
        if page > 0:
            nav_row.append(types.InlineKeyboardButton(
                "⬅️ Назад", callback_data=users_page_cb.new(action='prev', page=page-1)
            ))
        if len(users) == 10:
            nav_row.append(types.InlineKeyboardButton(
                "Вперед ➡️", callback_data=users_page_cb.new(action='next', page=page+1)
            ))
        if nav_row:
            keyboard.row(*nav_row)

        # Кнопки действий
        keyboard.add(types.InlineKeyboardButton(
            "🔄 Обновить", callback_data=users_page_cb.new(action='refresh', page=page)
        ))
        keyboard.add(types.InlineKeyboardButton(
            "🔙 В админ-панель", callback_data="back_to_admin"
        ))

        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await callback_query.answer()

    except Exception as e:
        bot_logger.error(f"Error in show_users_list: {e}")
        await callback_query.answer("Ошибка при получении списка пользователей", show_alert=True)

# Добавляем обработчик для просмотра информации о пользователе
send_to_user_cb = CallbackData('send', 'action', 'user_id')

# Изменяем формирование кнопки в show_user_info
# Обновляем обработчик для кнопки "Назад" в информации о пользователе
@dp.callback_query_handler(user_info_cb.filter(action='info'), state='*')
async def show_user_info(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return

    # Если есть активное состояние, сбрасываем его
    current_state = await state.get_state()
    if current_state:
        await state.finish()

    try:
        user_id = int(callback_data['id'])
        user = await execute_db_query(USERSDATABASE, """
            SELECT telegram_id, email, referrer_id, referral_count, 
                   free_configs_count, total_referrals, first_name, username,
                   registration_date
            FROM users 
            WHERE telegram_id = ?
        """, (user_id,))
        
        if not user:
            await callback_query.answer("Пользователь не найден", show_alert=True)
            return

        user = user[0]
        telegram_id, email, referrer_id, ref_count, free_conf, total_refs, first_name, username, reg_date = user
        
        text = (
            f"👤 <b>Информация о пользователе</b>\n\n"
            f"🆔 ID: <code>{telegram_id}</code>\n"
            f"📝 Имя: {first_name or 'Не указано'}\n"
            f"👤 Username: @{username or 'Нет'}\n"
            f"📧 Email: {email or 'Не указан'}\n"
            f"👥 Рефералов: {total_refs}\n"
            f"🎁 Бонусов: {free_conf}\n"
            f"📅 Дата регистрации: {reg_date or 'Неизвестно'}\n"
            f"🔗 Реферер: {referrer_id or 'Нет'}"
        )

        keyboard = types.InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            types.InlineKeyboardButton(
                "✉️ Написать", 
                callback_data=send_to_user_cb.new(action='write', user_id=user_id)
            ),
            types.InlineKeyboardButton("🔙 К списку", callback_data="users_list")
        )

        await callback_query.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await callback_query.answer()

    except Exception as e:
        bot_logger.error(f"Error in show_user_info: {e}")
        await callback_query.answer("Ошибка при получении информации о пользователе", show_alert=True)

class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_message_to_user = State()

user_info_cb = CallbackData('user_info', 'action', 'id')
send_to_user_cb = CallbackData('send', 'action', 'user_id')
# Обновляем обработчик отправки сообщения пользователю
@dp.callback_query_handler(send_to_user_cb.filter(action='write'))
async def start_write_to_user(callback_query: types.CallbackQuery, callback_data: dict):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return
    
    user_id = int(callback_data['user_id'])
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton(
            "🔙 Назад", 
            callback_data=user_info_cb.new(action='info', id=user_id)  # Исправлено здесь
        )
    )
    
    await callback_query.message.edit_text(
        "✉️ <b>Отправка сообщения пользователю</b>\n\n"
        f"📝 Введите сообщение для отправки пользователю ID: {user_id}\n"
        "<i>Для отмены нажмите кнопку Назад</i>",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    
    await AdminStates.waiting_for_message_to_user.set()
    state = dp.current_state(user=callback_query.from_user.id)
    await state.update_data(target_user_id=user_id)

@dp.message_handler(state=AdminStates.waiting_for_message_to_user)
async def process_message_to_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        return
    
    async with state.proxy() as data:
        target_user_id = data.get('target_user_id')
    
    try:
        await bot.send_message(target_user_id, 
            f"📨 <b>Сообщение от администратора:</b>\n\n{message.text}",
            parse_mode=ParseMode.HTML
        )
        await message.answer("✅ Сообщение успешно отправлено!")
        await state.finish()
        
        # Возвращаемся к списку пользователей
        await show_users_list(message, page=0)
        
    except Exception as e:
        bot_logger.error(f"Error sending message to user: {e}")
        await message.answer("❌ Ошибка при отправке сообщения")
        await state.finish()

@dp.callback_query_handler(users_page_cb.filter())
async def process_users_page(callback_query: types.CallbackQuery, callback_data: dict):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return

    action = callback_data['action']
    page = int(callback_data['page'])

    if action in ['next', 'prev', 'refresh']:
        await show_users_list(callback_query, page)

@dp.callback_query_handler(lambda c: c.data == "send_to_user")
async def start_send_to_user(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin"))
    
    await callback_query.message.edit_text(
        "✉️ <b>Отправка сообщения пользователю</b>\n\n"
        "Введите ID пользователя:\n"
        "<i>Для отмены нажмите кнопку Назад</i>",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    await AdminStates.waiting_for_user_id.set()

# Добавляем обработчик для кнопки "Назад" в состояниях AdminStates
@dp.callback_query_handler(lambda c: c.data == "back_to_admin", state=[AdminStates.waiting_for_user_id, AdminStates.waiting_for_message_to_user])
async def back_to_admin_from_send(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа", show_alert=True)
        return
    
    # Сбрасываем состояние
    await state.finish()
    
    # Возвращаемся в админ-панель
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="📢 Создать рассылку", callback_data="start_broadcast"))
    keyboard.add(types.InlineKeyboardButton(text="📊 Статистика бота", callback_data="bot_stats"))
    keyboard.add(types.InlineKeyboardButton(text="👤 Список пользователей", callback_data="users_list"))
    keyboard.add(types.InlineKeyboardButton(text="✉️ Написать пользователю", callback_data="send_to_user"))
    keyboard.add(types.InlineKeyboardButton(text="🔄 Перезагрузить бота", callback_data="restart_bot"))
    
    await callback_query.message.edit_text(
        "╔═══ ⚙️ АДМИН-ПАНЕЛЬ ⚙️ ═══╗\n\n"
        "👨‍💻 <b>УПРАВЛЕНИЕ БОТОМ:</b>\n\n"
        "├─ 📢 Рассылка сообщений\n"
        "├─ 📊 Просмотр статистики\n"
        "├─ 👤 Список пользователей\n"
        "├─ ✉️ Отправка сообщений\n"
        "└─ 🔄 Перезагрузка бота",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    await callback_query.answer()

@dp.message_handler(state=AdminStates.waiting_for_user_id)
async def process_user_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        return
    
    try:
        user_id = int(message.text)
        user = await execute_db_query(USERSDATABASE, "SELECT telegram_id FROM users WHERE telegram_id = ?", (user_id,))
        
        if not user:
            await message.answer("❌ Пользователь не найден")
            return
        
        await state.update_data(target_user_id=user_id)
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin"))
        
        await message.answer(
            f"👤 Пользователь найден (ID: {user_id})\n"
            "✍️ Введите сообщение для отправки:",
            reply_markup=keyboard
        )
        await AdminStates.waiting_for_message_to_user.set()
        
    except ValueError:
        await message.answer("❌ Некорректный ID пользователя")
    except Exception as e:
        bot_logger.error(f"Error in process_user_id: {e}")
        await message.answer("❌ Произошла ошибка")

@dp.message_handler(state=AdminStates.waiting_for_message_to_user)
async def send_message_to_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        return
    
    async with state.proxy() as data:
        user_id = data['target_user_id']
    
    try:
        await bot.send_message(
            user_id,
            f"📨 <b>Сообщение от администратора:</b>\n\n{message.text}",
            parse_mode=ParseMode.HTML
        )
        await message.answer("✅ Сообщение успешно отправлено")
    except Exception as e:
        bot_logger.error(f"Error sending message to user {user_id}: {e}")
        await message.answer("❌ Ошибка при отправке сообщения")
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "bot_stats")
async def show_bot_stats(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ У вас нет доступа к этой функции", show_alert=True)
        return

    try:
        # Получаем общее количество пользователей
        total_users = await execute_db_query(USERSDATABASE, "SELECT COUNT(*) FROM users")
        total_users = total_users[0][0] if total_users else 0

        # Получаем количество активных пользователей (с email)
        active_users = await execute_db_query(USERSDATABASE, "SELECT COUNT(*) FROM users WHERE email IS NOT NULL AND email != ''")
        active_users = active_users[0][0] if active_users else 0

        # Получаем общее количество рефералов
        total_referrals = await execute_db_query(USERSDATABASE, "SELECT SUM(total_referrals) FROM users")
        total_referrals = total_referrals[0][0] if total_referrals and total_referrals[0][0] else 0

        # Получаем количество выданных бесплатных конфигураций
        free_configs = await execute_db_query(USERSDATABASE, "SELECT SUM(free_configs_count) FROM users")
        free_configs = free_configs[0][0] if free_configs and free_configs[0][0] else 0

        # Добавляем временную метку для предотвращения дублирования сообщений
        timestamp = int(time.time())
        
        stats_text = (
            "╔═══ 📊 СТАТИСТИКА БОТА 📊 ═══╗\n\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"✅ <b>Активных пользователей:</b> {active_users}\n"
            f"🤝 <b>Всего рефералов:</b> {total_referrals}\n"
            f"🎁 <b>Выдано бесплатных конфигураций:</b> {free_configs}\n\n"
            f"🕒 Обновлено: {dt.fromtimestamp(timestamp).strftime('%H:%M:%S')}"
        )

        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton("🔄 Обновить", callback_data=f"bot_stats"))
        keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin"))

        try:
            await callback_query.message.edit_text(
                stats_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        except aiogram_exceptions.MessageNotModified:
            # Если сообщение не изменилось, просто показываем уведомление
            await callback_query.answer("Статистика актуальна", show_alert=False)
        else:
            await callback_query.answer()

    except Exception as e:
        bot_logger.error(f"Error in show_bot_stats: {e}")
        await callback_query.answer("Произошла ошибка при получении статистики", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "back_to_admin")
async def back_to_admin_panel(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ У вас нет доступа к этой функции", show_alert=True)
        return

    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="📢 Создать рассылку", callback_data="start_broadcast"))
    keyboard.add(types.InlineKeyboardButton(text="📊 Статистика бота", callback_data="bot_stats"))
    keyboard.add(types.InlineKeyboardButton(text="👤 Список пользователей", callback_data="users_list"))
    keyboard.add(types.InlineKeyboardButton(text="✉️ Написать пользователю", callback_data="send_to_user"))
    keyboard.add(types.InlineKeyboardButton(text="🔄 Перезагрузить бота", callback_data="restart_bot"))
    
    await callback_query.message.edit_text(
        "╔═══ ⚙️ АДМИН-ПАНЕЛЬ ⚙️ ═══╗\n\n"
        "👨‍💻 <b>УПРАВЛЕНИЕ БОТОМ:</b>\n\n"
        "├─ 📢 Рассылка сообщений\n"
        "├─ 📊 Просмотр статистики\n"
        "├─ 👤 Список пользователей\n"
        "├─ ✉️ Отправка сообщений\n"
        "└─ 🔄 Перезагрузка бота",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    await callback_query.answer()

@dp.message_handler(lambda message: message.text == "🌟 Реферальная система")
@subscription_required
async def handle_referral_program(message: types.Message, **kwargs):
    await message.delete()
    try:
        user_id = message.from_user.id
        referral_link = await get_referral_link(user_id)
        total_referrals = await get_total_referrals(user_id)
        
        discount = min(total_referrals * 3, 20) 
        
        free_configs = await get_available_free_configs(user_id)
        
        text = (
            "╔═ 👑 РЕФЕРАЛЬНАЯ СИСТЕМА 👑 ═╗\n\n"
            "📊 <b>ВАША СТАТИСТИКА:</b>\n"
            f"├─ 👥 Приглашено друзей: {total_referrals}\n"
            f"├─ 💎 Текущая скидка: {discount}%\n"
            f"└─ 🎁 Доступно бонусов: {free_configs}\n\n"
            "🌟 <b>БОНУСНАЯ ПРОГРАММА:</b>\n"
            "├─ 💰 +3% скидки за каждого друга\n"
            "├─ ⭐️ Максимальная скидка: 20%\n"
            "├─ 🎯 Каждые 5 друзей = 1 конфиг\n"
            "└─ 📦 Бонусный конфиг: 5 ГБ\n\n"
            "🔗 <b>ВАША РЕФЕРАЛЬНАЯ ССЫЛКА:</b>\n"
            f"└─ <code>{referral_link}</code>\n\n"
            "💫 <i>Приглашайте друзей и получайте бонусы!</i>"
        )
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(
            text="🚀 Пригласить друга и получить бонус",
            switch_inline_query=f"🔒 Безопасный и быстрый VPN! Присоединяйся по моей ссылке: {referral_link}"
        ))
        if free_configs > 0:
            keyboard.add(types.InlineKeyboardButton(text="🎁 Получить бесплатный конфиг", callback_data="get_free_config"))
        
        await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        bot_logger.error(f"Ошибка в handle_referral_program: {e}")
        await message.answer("Произошла ошибка при получении информации о реферальной программе. Пожалуйста, попробуйте позже.")

async def get_available_free_configs(user_id):
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT referral_count, free_configs_count FROM users WHERE telegram_id = ?', (user_id,))
        result = await cursor.fetchone()
        if result:
            referral_count, free_configs_count = result
            available_configs = (referral_count // 5) - free_configs_count
            return max(0, available_configs)
        return 0

async def apply_referral_discount(user_id, price):
    discount = await get_referral_discount(user_id)
    discounted_price = price * (1 - discount / 100)
    return round(discounted_price)

async def get_total_referrals(user_id):
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT total_referrals FROM users WHERE telegram_id = ?', (user_id,))
        result = await cursor.fetchone()
        return result[0] if result else 0

@dp.callback_query_handler(purchase_cb.filter(action="purchase"), state=AddClient.WaitingForExpiryTime)
async def purchase_subscription(callback_query: types.CallbackQuery, state: FSMContext):
    async with state.proxy() as data:
        name = data['name']
        expiry_time = data['expiry_time']

    expiry_time_text = get_expiry_time_description(expiry_time)
    price_index = {"-2592000000": 0, "-5184000000": 1, "-15552000000": 2, "-31104000000": 3}[str(expiry_time)]
    original_price = PRICES[price_index].amount
    
    # Получаем скидку для пользователя
    discount_percent = await get_referral_discount(callback_query.from_user.id)
    
    # Применяем скидку
    discounted_price = int(original_price * (1 - discount_percent / 100))
    
    bot_logger.info(f"Original price: {original_price}, Discount: {discount_percent}%, Discounted price: {discounted_price}")

    invoice_payload = json.dumps({"name": name, "expiry_time": expiry_time})
    
    await bot.send_invoice(
        callback_query.from_user.id,
        title=f"Подписка для {name} на {expiry_time_text}",
        description=f"Подписка на {expiry_time_text} со скидкой {discount_percent}%",
        provider_token=PAYMENTS_TOKEN,
        currency="rub",
        prices=[types.LabeledPrice(label=f"Подписка со скидкой {discount_percent}%", amount=discounted_price)],
        start_parameter=f"vpn_subscription_{callback_query.from_user.id}_{expiry_time}",
        payload=invoice_payload
    )
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, "⚠️ Ошиблись? Не оплачивайте. Начните заново.")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == 'copy_referral_link')
async def copy_referral_link(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    referral_link = await get_referral_link(user_id)
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Поделиться ссылкой", switch_inline_query=f"Присоединяйся к нашему VPN сервису! {referral_link}"))
    
    await callback_query.answer()
    await callback_query.message.answer(
        f"Ваша реферальная ссылка:\n\n<code>{referral_link}</code>\n\n"
        "Вы можете скопировать эту ссылку вручную или использовать кнопку ниже, чтобы поделиться ею.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data == 'get_free_config')
async def get_free_config(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    available_configs = await get_available_free_configs(user_id)
    
    if available_configs > 0:
        config_name = await create_free_config(user_id)
        if config_name:
            await callback_query.message.answer("🎉 Поздравляем! Ваш бесплатный VPN-конфиг активирован! 🚀")
            await send_config(callback_query.message, config_name)
            await update_free_configs_count(user_id)
            new_discount = await get_referral_discount(user_id)
            await callback_query.answer(f"🎉 Бесплатный конфиг успешно получен! Ваша текущая скидка: {new_discount}%", show_alert=True)
        else:
            await callback_query.answer("Произошла ошибка при создании конфига. Попробуйте позже.", show_alert=True)
    else:
        await callback_query.answer(f"У вас нет доступных бесплатных конфигураций. Пригласите больше друзей!", show_alert=True)

async def update_free_configs_count(user_id):
    async with aiosqlite.connect(USERSDATABASE) as db:
        await db.execute('UPDATE users SET free_configs_count = free_configs_count + 1 WHERE telegram_id = ?', (user_id,))
        await db.commit()

async def reduce_free_configs_count(user_id):
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT referral_count FROM users WHERE telegram_id = ?', (user_id,))
        result = await cursor.fetchone()
        if result:
            current_referral_count = result[0]
            free_configs_count = current_referral_count // 5
            new_free_configs_count = max(free_configs_count - 1, 0)
            new_referral_count = current_referral_count - (free_configs_count - new_free_configs_count) * 5
            await db.execute('UPDATE users SET referral_count = ? WHERE telegram_id = ?', (new_referral_count, user_id))
            await db.commit()
            bot_logger.info(f"Уменьшено количество бесплатных конфигураций для пользователя {user_id}. Было: {free_configs_count}, стало: {new_free_configs_count}")
        else:
            bot_logger.error(f"Пользователь {user_id} не найден в базе данных")

async def get_referral_count(user_id):
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT referral_count FROM users WHERE telegram_id = ?', (user_id,))
        result = await cursor.fetchone()
        return result[0] if result else 0

async def get_referral_link(user_id):
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={user_id}"
    bot_logger.info(f"Generated referral link for user {user_id}: {link}")
    return link

async def create_free_config(user_id):
    config_name = f"free_config_{user_id}_{int(time.time())}"
    expiry_time = -259200000  # 3 дня в миллисекундах
    total_gb = 5 * 1024 ** 3  # 5 ГБ в байтах
    
    session_id = await login()
    if session_id:
        client_id = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
        id_vless = ID_3  
        
        settings = {
            "clients": [{
                "id": str(client_id),
                "alterId": 0,
                "flow": "xtis-rprx-vision",
                "email": str(config_name),
                "limitIp": 3,
                "totalGB": total_gb,
                "expiryTime": expiry_time,
                "enable": True,
                "tgId": "",
                "subId": ""
            }]
        }
        
        client_data = {
            "id": int(id_vless),
            "settings": json.dumps(settings)
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cookie": f"3x-ui={session_id}"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(ADD_CLIENT_URL, headers=headers, json=client_data, ssl=False) as response:
                if response.status == 200:
                    response_json = await response.json()
                    if response_json.get("success"):
                        try:
                            existing_user = await execute_db_query(USERSDATABASE, "SELECT email FROM users WHERE telegram_id = ?", (user_id,))
                            if existing_user:
                                current_email = existing_user[0][0]
                                new_email = f"{current_email},{config_name}" if current_email else config_name
                                await execute_db_query(USERSDATABASE, "UPDATE users SET email = ? WHERE telegram_id = ?", (new_email, user_id))
                            else:
                                await execute_db_query(USERSDATABASE, "INSERT INTO users (telegram_id, email) VALUES (?, ?)", (user_id, config_name))
                            return config_name
                        except Exception as e:
                            bot_logger.error(f"Error updating/inserting user data: {e}")
                            return None
                    else:
                        bot_logger.error(f"Failed to add client: {response_json.get('msg')}")
                        return None
                else:
                    bot_logger.error(f"Failed to add client. Status: {response.status}")
                    return None
    else:
        bot_logger.error("Failed to login")
        return None

async def update_referral_count(referrer_id):
    try:
        async with aiosqlite.connect(USERSDATABASE) as db:
            cursor = await db.execute('SELECT referral_count, total_referrals FROM users WHERE telegram_id = ?', (referrer_id,))
            before = await cursor.fetchone()
            await db.execute('''
                UPDATE users 
                SET referral_count = referral_count + 1,
                    total_referrals = total_referrals + 1
                WHERE telegram_id = ?
            ''', (referrer_id,))
            await db.commit()
            cursor = await db.execute('SELECT referral_count, total_referrals FROM users WHERE telegram_id = ?', (referrer_id,))
            after = await cursor.fetchone()
            bot_logger.info(f"Referral count updated for user {referrer_id}. Before: {before}, After: {after}")
    except Exception as e:
        bot_logger.error(f"Error updating referral count for user {referrer_id}: {e}")

async def get_referral_discount(user_id):
    try:
        total_referrals = await get_total_referrals(user_id)
        if total_referrals is None:
            bot_logger.error(f"Не удалось получить количество рефералов для пользователя {user_id}")
            return 0
        
        discount = min(total_referrals * 3, 20)  # 3% за каждого реферала, максимум 20%
        bot_logger.info(f"Рассчитана скидка для пользователя {user_id}: {discount}% (количество рефералов: {total_referrals})")
        return discount
    except Exception as e:
        bot_logger.error(f"Ошибка при расчете скидки для пользователя {user_id}: {e}")
        return 0

@dp.message_handler(commands=['start'])
async def start(message: types.Message, **kwargs):
    user_id = message.from_user.id
    referrer_id = message.get_args()
    current_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
    
    bot_logger.info(f"Start command received. User ID: {user_id}, Referrer ID: {referrer_id}")
    
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT referrer_id FROM users WHERE telegram_id = ?', (user_id,))
        existing_user = await cursor.fetchone()
        
        if not existing_user:
            # Новый пользователь
            if referrer_id and referrer_id.isdigit() and int(referrer_id) != user_id:
                referrer_id = int(referrer_id)
                await db.execute('''
                    INSERT INTO users (
                        telegram_id, referrer_id, first_name, username, registration_date
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    user_id, 
                    referrer_id,
                    message.from_user.first_name,
                    message.from_user.username,
                    current_time
                ))
                await db.commit()
                await update_referral_count(referrer_id)
                bot_logger.info(f"New user {user_id} added with referrer {referrer_id}")
                try:
                    await bot.send_message(referrer_id, 
                        "╔═══ 🎉 НОВЫЙ РЕФЕРАЛ! 🎉 ═══╗\n\n"
                        "👤 Новый пользователь присоединился\n"
                        "💎 по вашей реферальной ссылке!\n\n"
                        "══════════════════════",
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    bot_logger.error(f"Failed to send message to referrer {referrer_id}: {e}")
            else:
                await db.execute('''
                    INSERT INTO users (
                        telegram_id, first_name, username, registration_date
                    ) VALUES (?, ?, ?, ?)
                ''', (
                    user_id,
                    message.from_user.first_name,
                    message.from_user.username,
                    current_time
                ))
                await db.commit()
                bot_logger.info(f"New user {user_id} added without referrer")

    if await check_subscription(user_id):
        # Пользователь подписан на канал
        keyboard = await get_main_keyboard(user_id)
        server_status = await check_server()
        
        welcome_text = (
            "╔══════ 🌟  YUKI VPN 🌟 ══════╗\n\n"
            f"     {server_status}\n\n"
            "✨ <b>ДОСТУПНЫЕ ФУНКЦИИ:</b>\n\n"
            "├─ 💎 Премиум VPN-подключения\n"
            "├─ 🚀 Высокая скорость доступа\n"
            "├─ 🔒 Надёжное шифрование\n"
            "└─ 💫 Бонусная программа\n\n"
            "🛡️ <b>ПРЕИМУЩЕСТВА:</b>\n\n"
            "├─ 📱 Поддержка всех устройств\n"
            "├─ ⚡️ Стабильное соединение\n"
            "├─ 🎯 Без ограничений\n"
            "└─ 🤝 Техподдержка 24/7\n\n"
            "👇 <b>Выберите нужный раздел меню:</b>"
        )
        
        await message.answer(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    else:
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK))
        keyboard.add(types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription"))

        message_text = (
            "╔═══ 🔒 ДОСТУП ОГРАНИЧЕН 🔒 ═══╗\n\n"
            "📱 <b>Для доступа к VPN боту</b>\n"
            f"└─ Подпишитесь на канал {CHANNEL_ID}\n\n"
            "🌟 <b>ПРЕИМУЩЕСТВА ПОДПИСКИ:</b>\n"
            "├─ 📢 Актуальные новости и обновления\n"
            "├─ 💎 Эксклюзивные предложения\n"
            "├─ 🛡️ Советы по безопасности\n"
            "├─ 🎁 Специальные акции\n"
            "└─ ⚡️ Быстрый доступ к обновлениям\n\n"
            "╔═══ ⚠️ ВАЖНО ⚠️ ═══╗\n"
            "├─ Подписка обязательна\n"
            "└─ Это займет 5 секунд\n\n"
            "👇 <b>Нажмите на кнопку ниже:</b>"
        )
        
        await message.answer(message_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


@dp.callback_query_handler(lambda c: c.data == 'check_subscription')
async def check_sub(callback_query: types.CallbackQuery):
    if await check_subscription(callback_query.from_user.id):
        await bot.answer_callback_query(callback_query.id, "Спасибо за подписку! Теперь вы можете использовать бота.")
        await main_menu(callback_query.message)
    else:
        await bot.answer_callback_query(callback_query.id, "Вы еще не подписались на канал. Пожалуйста, подпишитесь для использования бота.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "start_broadcast")
async def broadcast_command(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_ID:
        await callback_query.answer("⛔️ Нет доступа!", show_alert=True)
        return
        
    await callback_query.message.answer(
        "╔═══ 📢 РАССЫЛКА 📢 ═══╗\n\n"
        "✍️ Отправьте сообщение для рассылки\n"
        "└─ Поддерживается HTML разметка\n\n"
        "❌ Для отмены напишите /cancel"
    )
    await BroadcastStates.waiting_for_message.set()
    await callback_query.answer()

# Создайте класс состояний для рассылки:
class BroadcastStates(StatesGroup):
    waiting_for_message = State()

# Добавьте обработчик сообщения для рассылки:
@dp.message_handler(state=BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_ID:
        return
        
    if message.text == "/cancel":
        await state.finish()
        await message.answer("❌ Рассылка отменена")
        return
        
    progress_msg = await message.answer("⏳ Начинаем рассылку...")
    
    success = 0
    fail = 0

    async with aiosqlite.connect(USERSDATABASE) as db:
        async with db.execute('SELECT telegram_id FROM users') as cursor:
            users = await cursor.fetchall()
            
            for user in users:
                try:
                    await bot.send_message(
                        user[0],
                        message.text,
                        parse_mode=ParseMode.HTML
                    )
                    success += 1
                except Exception as e:
                    fail += 1
                    bot_logger.error(f"Failed to send broadcast to {user[0]}: {e}")
                
                if (success + fail) % 25 == 0:  # Обновляем статус каждые 25 сообщений
                    await progress_msg.edit_text(
                        f"⏳ Отправлено: {success + fail}/{len(users)}\n"
                        f"✅ Успешно: {success}\n"
                        f"❌ Ошибок: {fail}"
                    )
    
    await progress_msg.edit_text(
        "╔═══ 📊 ИТОГИ РАССЫЛКИ 📊 ═══╗\n\n"
        f"📨 Всего отправлено: {success + fail}\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибок: {fail}"
    )
    
    await state.finish()

@dp.message_handler(lambda message: message.text == "💎 Тарифы")
@subscription_required
async def handle_tariffs_and_purchase(message: types.Message, **kwargs):
    await message.delete()
    await show_tariffs_and_purchase(message)

@dp.message_handler(lambda message: message.text == "📚 Инструкция")
@subscription_required
async def handle_instructions(message: types.Message, **kwargs):
    await message.delete()
    await show_instructions(message)

@dp.message_handler(lambda message: message.text == "👤 Профиль")
@subscription_required
async def handle_get_config(message: types.Message, **kwargs):
    await message.delete()
    await start_get_config(message)

@dp.message_handler(lambda message: message.text == "🌐 Правила")
@subscription_required
async def handle_server_info(message: types.Message, **kwargs):
    await message.delete()
    await show_server_info(message)

@dp.message_handler(lambda message: message.text == "🎁 ПРОБНЫЙ ПЕРИОД • 3 ДНЯ")
@subscription_required
async def start_trial_period_button_handler(message: types.Message, **kwargs):
    await message.delete()
    await start_trial_period(message, dp.current_state(chat=message.from_user.id, user=message.from_user.id))

async def show_tariffs_and_purchase(message: types.Message):
    prices_text = (
        "╔═══ 💎 VPN ТАРИФЫ 💎 ═══╗\n\n"
        "💰 <b>СТОИМОСТЬ ПОДКЛЮЧЕНИЯ:</b>\n"
        f"├─ 🔸 1 месяц: <b>{os.getenv('PRICE_2592000000')}₽</b>\n"
        f"├─ 🔸 2 месяца: <b>{os.getenv('PRICE_5184000000')}₽</b>\n"
        f"├─ 🔸 6 месяцев: <b>{os.getenv('PRICE_15552000000')}₽</b>\n"
        f"└─ 🔸 1 год: <b>{os.getenv('PRICE_31104000000')}₽</b>\n\n"
        "🌟 <b>ПРЕИМУЩЕСТВА СЕРВИСА:</b>\n"
        "├─ ⚡️ Максимальная скорость\n"
        "├─ 📊 Безлимитный трафик\n"
        "├─ 🌍 Обход блокировок\n"
        "├─ 🔒 Надежное шифрование\n"
        "└─ 📱 Все устройства\n\n"
        "✨ <b>ДОПОЛНИТЕЛЬНО:</b>\n"
        "├─ 🔄 Гарантия возврата 30 дней\n"
        "├─ 🎁 Бонусная программа\n"
        f"└─ 👨‍💻 Поддержка 24/7: {NIK}\n\n"
        "🛡️ <i>Безопасность и скорость в одном пакете!</i>"
    )
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Купить VPN 🛒", callback_data="buy_vpn"))
    
    await message.answer(prices_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data == 'buy_vpn')
async def process_buy_vpn(callback_query: types.CallbackQuery):
    if await check_subscription(callback_query.from_user.id):
        await bot.answer_callback_query(callback_query.id)
        await start_add_client(callback_query.message)
    else:
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            types.InlineKeyboardButton("📢 Подписаться на канал", url=CHANNEL_LINK),
            types.InlineKeyboardButton("✅ Проверить подписку", callback_data="check_subscription")
        )

        await bot.answer_callback_query(
            callback_query.id,
            f"⚠️ Для доступа к боту требуется подписка на наш канал {CHANNEL_ID}",
            show_alert=True
        )

        await bot.send_message(
            callback_query.from_user.id,
            (
                "╔═══ 🔒 ДОСТУП ОГРАНИЧЕН 🔒 ═══╗\n\n"
                "📱 <b>Для доступа к VPN боту</b>\n"
                f"└─ Подпишитесь на канал {CHANNEL_ID}\n\n"
                "🌟 <b>ПРЕИМУЩЕСТВА ПОДПИСКИ:</b>\n"
                "├─ 📢 Актуальные новости и обновления\n"
                "├─ 💎 Эксклюзивные предложения\n"
                "├─ 🛡️ Советы по безопасности\n"
                "├─ 🎁 Специальные акции\n"
                "└─ ⚡️ Быстрый доступ к обновлениям\n\n"
                "╔═══ ⚠️ ВАЖНО ⚠️ ═══╗\n"
                "├─ Подписка обязательна\n"
                "└─ Это займет 5 секунд\n\n"
                "👇 <b>Нажмите на кнопку ниже:</b>"
            ),
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )

async def show_instructions(message: types.Message):
    instruction_menu_keyboard = types.InlineKeyboardMarkup(row_width=2)

    instructions = [
        ("📱 iOS", "https://telegra.ph/Instrukciya-dlya-iOS-01-11"),
        ("🤖 Android", "https://telegra.ph/Instrukciya-dlya-Android-01-11"),
        ("🍎 macOS", "https://telegra.ph/Instrukciya-dlya-MacOS-01-11"),
        ("🖥️ Windows", "https://telegra.ph/Instrukciya-dlya-Windows-01-11"),
        ("🐧 Linux", "https://telegra.ph/Instrukciya-dlya-Linux-Ubuntu-AppImage-01-11"),
    ]

    for name, url in instructions:
        instruction_menu_keyboard.insert(types.InlineKeyboardButton(text=name, url=url))

    instruction_menu_keyboard.add(
        types.InlineKeyboardButton(text="❓ FAQ", callback_data="faq")
    )

    await message.answer(
        "📚 <b>Инструкции по настройке</b>\n\n"
        "Выберите вашу операционную систему для получения подробной инструкции:\n\n"
        "• iOS - для iPhone и iPad\n"
        "• Android - для смартфонов и планшетов\n"
        "• macOS - для компьютеров Apple\n"
        "• Windows - для ПК\n"
        "• Linux - для Ubuntu/Debian\n\n"
        "Если у вас остались вопросы после прочтения инструкции, "
        "воспользуйтесь разделом FAQ (часто задаваемые вопросы).",
        reply_markup=instruction_menu_keyboard,
        parse_mode=ParseMode.HTML
    )

@dp.callback_query_handler(lambda c: c.data == 'faq')
async def process_faq(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    faq_text = (
        "╔══════ ❓ FAQ ❓ ══════╗\n\n"
        "🔧 <b>НАСТРОЙКА VPN:</b>\n"
        "├─ Выберите устройство\n"
        "├─ Следуйте инструкции\n"
        "└─ Готово к работе!\n\n"
        "⚡️ <b>РЕШЕНИЕ ПРОБЛЕМ:</b>\n"
        "├─ Проверьте данные\n"
        "├─ Перезапустите приложение\n"
        "└─ Проверьте интернет\n\n"
        "🛡️ <b>БЕЗОПАСНОСТЬ:</b>\n"
        "├─ Без логирования\n"
        "├─ Защита данных\n"
        "└─ Обход блокировок\n\n"
        "👨‍💻 <b>ПОДДЕРЖКА 24/7:</b>\n"
        f"└─ {NIK}\n\n"
        "💫 <i>Остались вопросы? Мы всегда на связи!</i>"
    )
    await bot.send_message(callback_query.from_user.id, faq_text, parse_mode=ParseMode.HTML)

async def show_server_info(message: types.Message):
    server_info_text = (
        "╔════ 🌟 YUKI VPN 🌟 ════╗\n\n"
        "🛡️ <b>БЕЗОПАСНОСТЬ:</b>\n"
        "├─ 🔒 Защита данных\n"
        "├─ 🕵️‍♂️ Полная анонимность\n"
        "└─ 🔐 Современное шифрование\n\n"
        "⚡️ <b>ПРОИЗВОДИТЕЛЬНОСТЬ:</b>\n"
        "├─ 🚀 Высокая скорость\n"
        "├─ ♾️ Безлимитный трафик\n"
        "└─ 🌍 Серверы по всему миру\n\n"
        "📋 <b>ПРАВИЛА ИСПОЛЬЗОВАНИЯ:</b>\n"
        "├─ ⚖️ Соблюдение законов\n"
        "├─ 🤝 Ответственное использование\n"
        "└─ 🔑 Не передавать доступ\n\n"
        "👨‍💻 <b>ПОДДЕРЖКА 24/7:</b>\n"
        f"└─ {NIK}\n\n"
        "✨ <i>Ваш надежный проводник в мир безопасного интернета!</i>"
    )
    await message.answer(server_info_text, parse_mode=ParseMode.HTML)

async def start_trial_period(message: types.Message, state: FSMContext):
    session_id = await login()
    if session_id:
        telegram_id = message.from_user.id
        user = await execute_db_query(USERSDATABASE, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))

        if user and user[0][1]:
            await message.answer(
                "🔒 <b>Доступ к пробному периоду ограничен</b>\n\n"
                "Извините, но наши записи показывают, что вы уже использовали пробный период. 🕵️‍♂️\n\n"
                "🌟 <b>Почему бы не рассмотреть наши тарифы?</b>\n"
                "• Доступные цены\n"
                "• Высокая скорость\n"
                "• Безлимитный трафик\n\n",
                parse_mode=ParseMode.HTML
            )
            bot_logger.info(f"Пользователь {telegram_id} попытался повторно использовать пробный период.")
        else:
            num1 = random.randint(1, 20)
            num2 = random.randint(1, 20)
            correct_answer = num1 + num2
            await message.answer(
                "╔═══ 🎁 ПРОБНЫЙ ПЕРИОД 🎁 ═══╗\n\n"
                "🎯 <b>ПОЛУЧИТЕ 3 ДНЯ БЕСПЛАТНО!</b>\n\n"
                "📝 <b>Ваша задача:</b>\n"
                f"├─ 🧮 <code>{num1} + {num2} = ?</code>\n"
                "└─ ✍️ Введите ответ цифрами\n\n"
                "💫 <i>Решите пример и активируйте VPN прямо сейчас!</i>"
            , parse_mode=ParseMode.HTML)
            await TrialPeriodState.waiting_for_answer.set()
            await state.update_data(correct_answer=correct_answer, num_attempts=0)
    else:
        await message.answer("❌ Ошибка аутентификации.")
        bot_logger.error("Ошибка аутентификации при попытке начала пробного периода.")
        await state.finish()

@dp.message_handler(state=TrialPeriodState.waiting_for_answer)
async def process_user_answer(message: types.Message, state: FSMContext):
    data = await state.get_data()
    num_attempts = data.get('num_attempts', 0)

    if not message.text.isdigit():
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(text="🔙 Вернуться в меню", callback_data="back_to_main"))
        
        await message.answer(
            "╔═══ ⚠️ ОШИБКА ВВОДА ⚠️ ═══╗\n\n"
            "❌ Введено некорректное значение\n\n"
            "📝 <b>Требования:</b>\n"
            "└─ Используйте только цифры\n\n",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
        return

    user_answer = int(message.text)
    correct_answer = data.get('correct_answer')

    if user_answer == correct_answer:
        id_vless = ID_1
        email = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
        expiry_time = -259200000
        telegram_id = message.from_user.id
        
        user = await execute_db_query(USERSDATABASE, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
        
        if user:
            if user[0][1]:
                await message.answer("❌ У вас уже есть активированный пробный период.")
                await state.finish()
                return
            await execute_db_query(USERSDATABASE, "UPDATE users SET email=? WHERE telegram_id=?", (email, telegram_id))
        else:
            await execute_db_query(USERSDATABASE, "INSERT INTO users (telegram_id, email) VALUES (?, ?)", (telegram_id, email))
        
        await message.answer(f"✅ Пробный период активирован! \nВаш логин: {email}")
        await add_client(message, email, expiry_time, id_vless, telegram_id)
        bot_logger.info(f"Пользователь {telegram_id} начал использовать пробный период.")
        
        # Обновляем клавиатуру после активации пробного периода
        keyboard = await get_main_keyboard(telegram_id)

        # Отправляем новое меню
        await message.answer(
            "╔══════ 🌟 YUKI VPN 🌟 ══════╗\n\n"
            "✨ <b>ДОСТУПНЫЕ ФУНКЦИИ:</b>\n\n"
            "├─ 💎 Премиум VPN-подключения\n"
            "├─ 🚀 Высокая скорость доступа\n"
            "├─ 🔒 Надёжное шифрование\n"
            "└─ 💫 Бонусная программа\n\n"
            "👇 <b>Выберите нужный раздел меню:</b>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )

        # Отправляем и удаляем уведомление об обновлении
        notification = await message.answer(
            "╔══ 🔄 ОБНОВЛЕНИЕ ══╗\n"
            "  Меню обновлено!\n",
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(3)
        await notification.delete()

        await state.finish()
    else:
        num_attempts += 1
        await state.update_data(num_attempts=num_attempts)

        if num_attempts >= 3:
            await message.answer("❌ Вы ввели неправильный ответ три раза подряд. Попробуйте позже.")
            await state.finish()
        else:
            await message.answer("❌ Неправильный ответ на математическую задачу. Попробуйте снова.")

async def start_add_client(message: types.Message):
    await message.answer(
        "🚀 <b>Выберите тариф VPN</b>\n\n"
        "🔒 Безопасность и скорость на любой срок\n\n"
        "👇 Выберите подходящий вариант:",
        parse_mode=ParseMode.HTML,
        reply_markup=get_expiry_time_keyboard()  # Добавляем клавиатуру с кнопками
    )
    await AddClient.WaitingForExpiryTime.set()

@dp.callback_query_handler(lambda c: c.data == 'back_to_main', state='*')
async def process_back_to_main(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await main_menu(callback_query.message)
    await callback_query.message.delete()

@dp.callback_query_handler(lambda c: c.data == "back_to_tariffs", state=AddClient.WaitingForExpiryTime)
async def back_to_tariffs(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()  # Сбрасываем состояние
    await show_tariffs_and_purchase(callback_query.message)  # Возвращаем к тарифам и покупке

@dp.callback_query_handler(expiry_cb.filter(), state=AddClient.WaitingForExpiryTime)
async def process_expiry_time(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    expiry_time = int(callback_data['time'])
    await state.update_data(expiry_time=expiry_time)
    
    expiry_time_description = get_expiry_time_description(expiry_time)
    price = get_price(expiry_time)

    original_price = int(price.replace(" ₽", ""))
    discounted_price = await apply_referral_discount(callback_query.from_user.id, original_price)
    
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        f"✅ <b>Тариф выбран</b>\n\n"
        f"📅 {expiry_time_description}\n"
        f"💰 {discounted_price} ₽\n"
        f"🎁 Скидка: {int(100 - (discounted_price / original_price * 100))}%\n\n"
        f"👤 <b>Придумайте логин</b>\n"
        f"• Латинские буквы и цифры\n"
        f"• Без пробелов и символов\n\n"
        f"<i>Введите логин:</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()  # Добавляем клавиатуру с кнопкой "Назад"
    )
    
    await AddClient.WaitingForName.set()

def get_back_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_expiry"))
    return keyboard

class AddClient(StatesGroup):
    WaitingForExpiryTime = State()
    WaitingForName = State()
    WaitingForConfirmation = State() 

@dp.callback_query_handler(lambda c: c.data == "back_to_expiry", state=AddClient.WaitingForName)
async def back_to_expiry(callback_query: types.CallbackQuery, state: FSMContext):
    await state.finish()  # Сбрасываем состояние
    await start_add_client(callback_query.message)  # Возвращаем к выбору тарифа

@dp.message_handler(state=AddClient.WaitingForName)
async def process_name(message: types.Message, state: FSMContext):
    name = message.text.strip().lower()

    if not re.match('^[a-z0-9]+$', name):
        await message.answer(
            "⚠️ <b>Внимание!</b>\n\n"
            "Введенное имя содержит недопустимые символы.\n"
            "✅ Разрешены только латинские буквы в нижнем регистре и цифры.\n"
            "❌ Кириллица, заглавные буквы и специальные символы запрещены.\n\n"
            "🔄 Пожалуйста, введите имя еще раз:",
            parse_mode=ParseMode.HTML
        )
        return

    user_exists = await execute_db_query(DATABASE, "SELECT * FROM client_traffics WHERE email=?", (name,))
    if user_exists:
        await message.answer(
            "⚠️ <b>Имя уже занято</b>\n\n"
            f"К сожалению, имя <code>{name}</code> уже используется.\n"
            "🔄 Пожалуйста, придумайте другое имя и введите его:",
            parse_mode=ParseMode.HTML
        )
        return

    async with state.proxy() as data:
        expiry_time = data['expiry_time']
        data['name'] = name

    expiry_time_description = get_expiry_time_description(expiry_time)
    price = get_price(expiry_time)
    
    # Применяем скидку
    original_price = int(price.replace(" ₽", ""))
    discounted_price = await apply_referral_discount(message.from_user.id, original_price)
    
    text = (
        f"🎉 <b>Отлично! Вы почти у цели!</b>\n\n"
        f"📅 <b>Срок подключения:</b> {expiry_time_description}\n"
        f"💰 <b>Стоимость:</b> {discounted_price} ₽\n"
        f"🔑 <b>Ваш логин:</b> <code>{name}</code>\n\n"
        f"🛒 Готовы приобрести подключение?\n\n"
    )

    await message.answer(
        text,
        reply_markup=get_purchase_keyboard_with_cancel(),
        parse_mode=ParseMode.HTML
    )
    
    # Переходим в состояние ожидания подтверждения покупки
    await AddClient.WaitingForConfirmation.set()

@dp.callback_query_handler(purchase_cb.filter(action="purchase"), state=AddClient.WaitingForConfirmation)
async def confirm_purchase(callback_query: types.CallbackQuery, state: FSMContext):
    async with state.proxy() as data:
        name = data['name']
        expiry_time = data['expiry_time']

    expiry_time_text = get_expiry_time_description(expiry_time)
    price_index = {"-2592000000": 0, "-5184000000": 1, "-15552000000": 2, "-31104000000": 3}[str(expiry_time)]
    original_price = PRICES[price_index].amount
    discounted_price = await apply_referral_discount(callback_query.from_user.id, original_price)
    
    invoice_payload = json.dumps({"name": name, "expiry_time": expiry_time})
    
    await bot.send_invoice(
        callback_query.from_user.id,
        title=f"Подписка для {name} на {expiry_time_text}",
        description=f"Подписка на {expiry_time_text}",
        provider_token=PAYMENTS_TOKEN,
        currency="rub",
        prices=[types.LabeledPrice(label=f"Подписка со скидкой", amount=discounted_price)],
        start_parameter=f"vpn_subscription_{callback_query.from_user.id}_{expiry_time}",
        payload=invoice_payload
    )
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, "⚠️ Если передумали или ввели что-то не то, не оплачивайте, а просто начните заново")
    await state.finish()

@dp.callback_query_handler(purchase_cb.filter(action="cancel"), state=AddClient.WaitingForConfirmation)
async def cancel_purchase(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer("Вы отменили создание нового подключения.")
    await bot.send_message(callback_query.message.chat.id, "Вы отменили создание нового подключения.")
    await state.finish()

@dp.callback_query_handler(purchase_cb.filter(action="purchase"), state=AddClient.WaitingForExpiryTime)
async def purchase_subscription(callback_query: types.CallbackQuery, state: FSMContext):
    async with state.proxy() as data:
        name = data['name']
        expiry_time = data['expiry_time']

    expiry_time_text = get_expiry_time_description(expiry_time)
    price_index = {"-2592000000": 0, "-5184000000": 1, "-15552000000": 2, "-31104000000": 3}[str(expiry_time)]
    original_price = PRICES[price_index].amount
    discounted_price = await apply_referral_discount(callback_query.from_user.id, original_price)
    
    invoice_payload = json.dumps({"name": name, "expiry_time": expiry_time})
    
    await bot.send_invoice(
        callback_query.from_user.id,
        title=f"Подписка для {name} на {expiry_time_text}",
        description=f"Подписка на {expiry_time_text}",
        provider_token=PAYMENTS_TOKEN,
        currency="rub",
        prices=[types.LabeledPrice(label=f"Подписка со скидкой", amount=discounted_price)],
        start_parameter=f"vpn_subscription_{callback_query.from_user.id}_{expiry_time}",
        payload=invoice_payload
    )
    await callback_query.answer()
    await bot.send_message(callback_query.from_user.id, "⚠️ Если передумали или ввели что-то не то, не оплачивайте, а просто начните заново")
    await state.finish()

@dp.pre_checkout_query_handler(lambda query: True)
async def pre_checkout_query(pre_checkout_q: types.PreCheckoutQuery):
    try:
        await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)
        bot_logger.info(f"Pre-checkout query answered successfully for user {pre_checkout_q.from_user.id}")
    except Exception as e:
        bot_logger.error(f"Error in pre_checkout_query: {e}")
        await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=False, error_message="Произошла ошибка при обработке платежа. Пожалуйста, попробуйте позже.")

@dp.message_handler(content_types=ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment(message: types.Message):
    try:
        bot_logger.info(f"Received successful payment from user {message.from_user.id}")
        payload = message.successful_payment.invoice_payload
        payload_data = json.loads(payload)
        
        name = payload_data.get("name")
        expiry_time = payload_data.get("expiry_time")
        if name is None or expiry_time is None:
            await bot.send_message(message.chat.id, "❌ Ошибка: имя или срок действия подписки не указаны.")
            bot_logger.error(f"Error: name or expiry_time is None for user {message.from_user.id}")
            return

        telegram_id = message.from_user.id
        id_vless = ID_2
        await add_client(message, name, expiry_time, id_vless, telegram_id)
        bot_logger.info(f"Successfully added client for user {telegram_id}")
    except Exception as e:
        bot_logger.error(f"Error in successful_payment: {e}")
        await message.answer("Произошла ошибка при обработке платежа. Пожалуйста, свяжитесь с администратором.")

async def add_client(message, name, expiry_time, id_vless, telegram_id):
    try:
        session_id = await login()
        if session_id:
            if name is not None and expiry_time is not None:
                client_id = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
                success = await add_client_request(message, session_id, name, expiry_time, client_id, id_vless)
                if success:
                    try:
                        existing_user = await execute_db_query(USERSDATABASE, "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
                        if existing_user:
                            current_email = existing_user[0][1]
                            new_email = f"{current_email},{name}" if current_email else name
                            await execute_db_query(USERSDATABASE, "UPDATE users SET email = ? WHERE telegram_id = ?", (new_email, telegram_id))
                        else:
                            await execute_db_query(USERSDATABASE, "INSERT INTO users (telegram_id, email) VALUES (?, ?)", (telegram_id, name))
                        await send_config(message, name)
                    except Exception as e:
                        bot_logger.error(f"Error updating/inserting user data: {e}")
                        await message.answer("❌ Произошла ошибка при обновлении данных пользователя.")
                else:
                    await message.answer("❌ Не удалось добавить клиента.")
            else:
                await message.answer("❌ Ошибка: имя или срок действия подписки не указаны.")
        else:
            await message.answer("❌ Ошибка аутентификации.")
    except Exception as e:
        bot_logger.error(f"Error in add_client: {e}")
        await message.answer("Произошла ошибка при добавлении клиента. Пожалуйста, свяжитесь с администратором.")

async def login():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(LOGIN_URL, data=LOGIN_DATA, ssl=False) as response:
                if response.status == 200:
                    cookies = response.cookies
                    session_id = cookies.get("3x-ui").value if "3x-ui" in cookies else None
                    if session_id:
                        bot_logger.info(f"Session ID: {session_id}")
                        return session_id
                    else:
                        bot_logger.error("Session ID not found in cookies")
                        return None
                else:
                    bot_logger.error(f"Failed to log in, status code: {response.status}")
                    return None
    except Exception as e:
        bot_logger.error(f"Exception during login: {e}")
        return None

async def add_client_request(message, session_id, name, expiry_time, client_id, id_vless):
    total_gb = total_gb_values.get(expiry_time, UNLIMITED_TRAFFIC)
    
    settings = {
        "clients": [{
            "id": str(client_id),
            "flow": "xtis-rprx-vision",
            "alterId": 0,
            "email": str(name),
            "limitIp": 3,
            "totalGB": total_gb if total_gb != UNLIMITED_TRAFFIC else 0,
            "expiryTime": int(expiry_time),
            "enable": True,
            "tgId": "",
            "subId": ""
        }]
    }
    
    client_data = {
        "id": int(id_vless),
        "settings": json.dumps(settings)
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": f"3x-ui={session_id}"
    }

    bot_logger.info(f"Отправляемые данные клиента: {json.dumps(client_data, ensure_ascii=False, indent=2)}")

    async with aiohttp.ClientSession() as session:
        async with session.post(ADD_CLIENT_URL, headers=headers, json=client_data, ssl=False) as response:
            bot_logger.info(f"Add client response status: {response.status}")
            response_text = await response.text()
            bot_logger.info(f"Add client response body: {response_text}")

            if response.status == 200:
                response_json = json.loads(response_text)
                if response_json.get("success"):
                    if message:
                        await message.answer(
                            "🎉 <b>Поздравляем!</b>\n\n"
                            "🔓 Ваша подписка активирована\n\n"
                            "🚀 Приятного пользования нашим VPN-сервисом!\n"
                            "📞 Если у вас возникнут вопросы, обращайтесь в поддержку.",
                            parse_mode=ParseMode.HTML
                        )
                    return True
                else:
                    error_msg = response_json.get('msg', 'Неизвестная ошибка')
                    if message:
                        await message.answer(
                            "❌ <b>Ошибка при добавлении клиента</b>\n\n"
                            f"🔍 Детали: {error_msg}\n\n"
                            "🔄 Пожалуйста, попробуйте еще раз или обратитесь в поддержку.",
                            parse_mode=ParseMode.HTML
                        )
                    bot_logger.error(f"Ошибка при добавлении клиента: {error_msg}")
                    return False
            else:
                if message:
                    await message.answer(
                        "🚫 <b>Ошибка сервера</b>\n\n"
                        f"📊 Код ответа: {response.status}\n"
                        f"📝 Ответ сервера: {response_text}\n\n"
                        "👨‍💻 Наша команда уже работает над решением проблемы.\n"
                        "🕒 Пожалуйста, попробуйте позже.",
                        parse_mode=ParseMode.HTML
                    )
                bot_logger.error(f"Ошибка сервера при добавлении клиента. Статус: {response.status}, Ответ: {response_text}")
                return False

async def send_config(message, email):
    db = Database(DATABASE)
    client_ids = await db.get_ids_by_email(email)
    
    if not client_ids:
        await message.answer(
            "❌ <b>Ошибка получения конфигурации</b>\n\n"
            "К сожалению, не удалось получить ваши конфигурации из базы данных.\n"
            "Пожалуйста, обратитесь в службу поддержки для решения этой проблемы.",
            parse_mode=ParseMode.HTML
        )
        return

    await message.answer(
        "🔐 <b>Ваши VPN конфигурации готовы!</b>\n\n"
        "Ниже вы найдете текстовую конфигурацию и QR-код для быстрой настройки.\n"
        "Используйте то, что вам удобнее.\n\n"
        "<i>Совет: Сохраните эту информацию в надежном месте.</i>",
        parse_mode=ParseMode.HTML
    )

    for client_id in client_ids:
        config_str, _ = await generate_config(client_id, email)
        
        await message.answer(
            f"📋 <b>Текстовая конфигурация:</b>\n\n<pre>{config_str}</pre>",
            parse_mode=ParseMode.HTML
        )
        
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=4, border=4)
        qr.add_data(config_str)  # Передаем только строку конфигурации
        qr.make(fit=True)
        qr_image = qr.make_image(fill_color="black", back_color="white")
        
        with BytesIO() as qr_bytes:
            qr_image.save(qr_bytes)
            qr_bytes.seek(0)
            await message.answer_photo(
                qr_bytes,
                caption="📲 <b>QR-код для быстрой настройки</b>\n"
                        "Отсканируйте этот код в вашем VPN-клиенте для автоматической настройки.",
                parse_mode=ParseMode.HTML
            )

    instruction_keyboard = types.InlineKeyboardMarkup(row_width=2)
    instructions = [
        ("📱 iOS", "ios_instruction"),
        ("🤖 Android", "android_instruction"),
        ("🍎 macOS", "macos_instruction"),
        ("🖥️ Windows", "windows_instruction")
    ]

    for name, callback in instructions:
        instruction_keyboard.insert(types.InlineKeyboardButton(text=name, callback_data=callback))

    await message.answer(
        "📚 <b>Выберите вашу операционную систему для настройки:</b>",
        reply_markup=instruction_keyboard,
        parse_mode=ParseMode.HTML
    )

async def generate_config(client_id, email, prt=None, pot=None, pbk=None, fp=None, sni=None, sid=None, net=None, secur=None):
    if prt is None or pot is None or pbk is None or fp is None or sni is None or sid is None:
        db = Database(DATABASE)
        stream_settings = await db.get_stream_settings(email)
        if stream_settings:
            prt, pot, net, secur, pbk, fp, sni, sid = stream_settings[0]
        else:
            return "", None
    
    config_str = f"{prt}://{client_id}@{SERVER_IP}:{pot}?type={net}&security={secur}&pbk={pbk}&fp={fp}&sni={sni}&sid={sid}&spx=%2F#{email}"
    
    config_dict = {
        "username": client_id,
        "server_address": SERVER_IP,
        "port": pot,
        "protocol_type": net,
        "security_type": secur,
        "public_key": pbk,
        "fingerprint": fp,
        "server_name": sni,
        "session_id": sid,
        "routing_params": "/",
        "name": email
    }
    
    return config_str, config_dict

def get_price(expiry_time_ms):
    prices = {
        -2592000000: int(os.getenv('PRICE_2592000000')),
        -5184000000: int(os.getenv('PRICE_5184000000')),
        -15552000000: int(os.getenv('PRICE_15552000000')),
        -31104000000: int(os.getenv('PRICE_31104000000'))
    }
    return f"{prices.get(expiry_time_ms, 'неизвестно')} ₽"

def get_expiry_time_description(expiry_time_ms):
    descriptions = {
        -2592000000: "месяц",
        -5184000000: "два месяца",
        -15552000000: "пол года",
        -31104000000: "год"
    }
    return descriptions.get(expiry_time_ms, "Неизвестно")

UNLIMITED_TRAFFIC = float('inf')

def get_total_gb(value):
    gb_value = os.getenv(f'TOTAL_GB_{abs(value)}')
    if gb_value == 'unlimited':
        return UNLIMITED_TRAFFIC
    return int(gb_value)

total_gb_values = {
    -259200000: get_total_gb(259200000),
    -2592000000: get_total_gb(2592000000),
    -5184000000: get_total_gb(5184000000),
    -15552000000: get_total_gb(15552000000),
    -31104000000: get_total_gb(31104000000),
}

async def start_get_config(message):
    telegram_id = message.from_user.id
    emails = await get_emails_from_database(telegram_id)

    if emails:
        if len(emails) == 1:
            await process_email_for_config(message, emails[0])
        else:
            keyboard = types.InlineKeyboardMarkup(row_width=1)
            for email in emails:
                keyboard.add(types.InlineKeyboardButton(text=f"🔑 {email}", callback_data=f"show_config:{email}"))
            
            await message.answer(
                "📦 <b>Ваши VPN конфигурации</b>\n\n"
                "У вас несколько активных конфигураций. "
                "Выберите логин, чтобы просмотреть детали:\n\n"
                "<i>Нажмите на кнопку с нужным логином ниже:</i>",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
    else:
        await message.answer(
            "❌ <b>Конфигурации не найдены</b>\n\n"
            "К сожалению, не удалось найти ваших активных VPN конфигураций.\n\n"
            "Возможные пичины:\n"
            "• У вас нет активных подписок\n"
            "• Произошла ошибка в системе\n\n"
            "🔄 Попробуйте обновить данные через некоторое время или обратитесь в нашу службу поддержки для помощи.",
            parse_mode=ParseMode.HTML
        )

@dp.callback_query_handler(lambda c: c.data.startswith('show_config:'))
async def process_config_selection(callback_query: types.CallbackQuery):
    email = callback_query.data.split(':')[1]
    await process_email_for_config(callback_query.message, email)
    await callback_query.answer()

async def get_emails_from_database(telegram_id):
    user_data = await execute_db_query(USERSDATABASE, "SELECT email FROM users WHERE telegram_id=?", (telegram_id,))
    if user_data and user_data[0][0]:
        return list(set(user_data[0][0].split(',')))
    return []

class ConfigStates(StatesGroup):
    last_config = State()

async def process_email_for_config(message, email):
    db = Database(DATABASE)
    client_ids = await db.get_ids_by_email(email)
    if client_ids:
        configs_sent = set()
        for client_id in client_ids:
            config_str, config_dict = await generate_config(client_id, email)
            if config_str not in configs_sent:
                configs_sent.add(config_str)
                
                # Сохраняем последнюю конфигурацию в состояние
                state = dp.current_state(user=message.chat.id)
                await state.update_data(last_config_str=config_str, last_config_dict=config_dict)
                
                # Отправляем конфигурацию как обычно
                await message.answer(
                    f"🔐 <b>Конфигурация VPN для {email}:</b>\n\n<pre>{config_str}</pre>",
                    parse_mode=ParseMode.HTML
                )
                
                # Генерируем и отправляем QR-код
                qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=4, border=4)
                qr.add_data(config_str)  # Используем строку конфигурации
                qr.make(fit=True)
                qr_image = qr.make_image(fill_color="black", back_color="white")
                with BytesIO() as qr_bytes:
                    qr_image.save(qr_bytes)
                    qr_bytes.seek(0)
                    await message.answer_photo(
                        qr_bytes,
                        caption=f"📲 QR-код для конфигурации {email}"
                    )

        # Добавляем инструкции для разных платформ
        instruction_keyboard = types.InlineKeyboardMarkup(row_width=2)
        instructions = [
            ("📱 iOS", "ios_instruction"),
            ("🤖 Android", "android_instruction"),
            ("🍎 macOS", "macos_instruction"),
            ("🖥️ Windows", "windows_instruction")
        ]

        for name, callback in instructions:
            instruction_keyboard.insert(types.InlineKeyboardButton(text=name, callback_data=callback))

        await message.answer(
            "📚 <b>Выберите вашу операционную систему для настройки:</b>",
            reply_markup=instruction_keyboard,
            parse_mode=ParseMode.HTML
        )

        client_traffics = await db.get_client_traffics_by_email(email)
        if client_traffics:
            traffic_response = "\n📊 <b>Информация о текущей подписке:</b>\n\n"
            for result in client_traffics:
                email, up, down, expiry_time, total = result

                formatted_expiry_time = dt.fromtimestamp(abs(expiry_time) / 1000).strftime('%d.%m.%Y %H:%M')
                up_gb = up / (1024 ** 3)
                down_gb = down / (1024 ** 3)
                total_gb = total / (1024 ** 3)

                remaining_gb = max(0, total_gb - (up_gb + down_gb))
                
                traffic_response += (
                    f"╔═ 📊 <b>Статистика</b> ═╗\n\n"
                    f"👤 <b>Логин:</b> <code>{email}</code>\n\n"
                    f"📈 <b>Использование трафика:</b>\n"
                    f"├─ ↑ Отправлено: {up_gb:.2f} ГБ\n"
                    f"├─ ↓ Получено: {down_gb:.2f} ГБ\n"
                    f"└─ 📊 Всего: {up_gb + down_gb:.2f} ГБ\n\n"
                    f"⏳ <b>Срок действия до:</b>\n"
                    f"└─ 📅 {formatted_expiry_time}\n\n"
                    f"💾 <b>Лимиты трафика:</b>\n"
                    f"├─ 📦 Общий объем: {format_traffic(total)}\n"
                    f"└─ ✨ Осталось: {format_traffic(remaining_gb * 1024 ** 3)}\n\n"
                )
            
            await message.answer(traffic_response, parse_mode=ParseMode.HTML)
        else:
            await message.answer("❌ Информация о трафике не найдена.")
    else:
        await message.answer("❌ Период прошлой подписки окончен!")

def format_traffic(traffic):
    if traffic == UNLIMITED_TRAFFIC:
        return "Безлимит"
    else:
        return f"{traffic / (1024**3):.2f} ГБ"

async def check_subscription_expiry():
    current_date = datetime.datetime.today().date()
    bot_logger.info(f"Текущая дата: {current_date}")

    clients = await execute_db_query(USERSDATABASE, "SELECT telegram_id, email FROM users")

    for client in clients:
        telegram_id, email = client
        subscription_expiry_time = await execute_db_query(DATABASE, "SELECT expiry_time FROM client_traffics WHERE email=?", (email,))

        if subscription_expiry_time:
            expiry_time = subscription_expiry_time[0][0]
            formatted_expiry_time = dt.fromtimestamp(abs(expiry_time) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            expiry_date = datetime.datetime.strptime(formatted_expiry_time, "%Y-%m-%d %H:%M:%S").date()
            bot_logger.info(f"Проверка подписки для пользователя {telegram_id}, дата окончания: {expiry_date}")
            
            for i in range(7, 0, -1):
                notification_date = current_date + datetime.timedelta(days=i)
                if expiry_date == notification_date:
                    await send_expiry_notification(telegram_id, i)

async def send_expiry_notification(telegram_id, days_left):
    if days_left == 1:
        notification_text = "Приветствую ❗️\nВаша подписка завершится завтра. Пожалуйста, продлите её вовремя. ⏰"
    elif days_left == 0:
        notification_text = "Приветствую ❗️\nСегодня последний день вашей подписки. Пожалуйста, продлите её срок. ⏳"
    else:
        days_word = 'дней' if 5 <= days_left <= 20 else 'день' if days_left % 10 == 1 else 'дня' if 2 <= days_left % 10 <= 4 else 'дней'
        notification_text = f"Приветствую ❗️\nОсталось всего {days_left} {days_word} до окончания подписки. Пожалуйста, продлите её вовремя. ⏳"

    try:
        await bot.send_message(telegram_id, notification_text)
        bot_logger.info(f"Уведомление отправлено пользователю с ID {telegram_id}")
    except Exception as e:
        bot_logger.error(f"Ошибка при отправке уведомления пользователю с ID {telegram_id}: {e}")

@dp.message_handler(commands=['check_subscription'])
async def check_subscription_command(message: types.Message):
    await check_subscription_expiry()
    await message.answer("Проверка подписок выполнена")

async def check_referral_integrity():
    async with aiosqlite.connect(USERSDATABASE) as db:
        cursor = await db.execute('SELECT telegram_id, referral_count, total_referrals FROM users')
        users = await cursor.fetchall()
        for user in users:
            telegram_id, referral_count, total_referrals = user
            cursor = await db.execute('SELECT COUNT(*) FROM users WHERE referrer_id = ?', (telegram_id,))
            actual_count = (await cursor.fetchone())[0]
            if actual_count != referral_count or actual_count != total_referrals:
                bot_logger.warning(f"Referral count mismatch for user {telegram_id}. DB: {referral_count}/{total_referrals}, Actual: {actual_count}")
                await db.execute('UPDATE users SET referral_count = ?, total_referrals = ? WHERE telegram_id = ?', (actual_count, actual_count, telegram_id))
        await db.commit()

# Добавьте эту функцию в scheduled()
async def scheduled():
    while True:
        await check_subscription_expiry()
        await check_referral_integrity()
        bot_logger.info("Проверка подписок и целостности рефералов выполнена")
        await asyncio.sleep(86400)

async def on_startup(dp):
    bot_logger.info("Бот запущен")
    await initialize_database()
    asyncio.create_task(scheduled())

async def on_shutdown(dp):
    bot_logger.info("Бот остановлен")

@dp.callback_query_handler(lambda c: c.data in ['ios_instruction', 'android_instruction', 'macos_instruction', 'windows_instruction'])
async def process_instruction(callback_query: types.CallbackQuery, state: FSMContext):
    # Получаем сохраненную конфигурацию из состояния
    state_data = await state.get_data()
    config_str = state_data.get('last_config_str')
    config_dict = state_data.get('last_config_dict')
    
    # Если конфигурация не найдена в состоянии, пробуем получить её из базы данных
    if not config_str or not config_dict:
        telegram_id = callback_query.from_user.id
        user_data = await execute_db_query(USERSDATABASE, "SELECT email FROM users WHERE telegram_id=?", (telegram_id,))
        
        if not user_data or not user_data[0][0]:
            await callback_query.answer("Конфигурация не найдена. Пожалуйста, активируйте VPN сначала.")
            return

        emails = user_data[0][0].split(',')
        email = emails[0]
        
        db = Database(DATABASE)
        client_ids = await db.get_ids_by_email(email)
        
        if not client_ids:
            await callback_query.answer("Конфигурация не найдена.")
            return

        config_str, config_dict = await generate_config(client_ids[0], email)
        
        if not config_str or not config_dict:
            await callback_query.answer("Не удалось сгенерировать конфигурацию.")
            return
        
        # Сохраняем конфигурацию в состояние
        await state.update_data(last_config_str=config_str, last_config_dict=config_dict)

    # Создаем ссылку для подключения
    connect_link = (
        f"https://vpn-connector.netlify.app/?url=vless://{config_dict['username']}@{config_dict['server_address']}:{config_dict['port']}"
        f"?type={config_dict['protocol_type']}a_n_dsecurity={config_dict['security_type']}a_n_dpbk={config_dict['public_key']}"
        f"a_n_dfp={config_dict['fingerprint']}a_n_dsni={config_dict['server_name']}a_n_dsid={config_dict['session_id']}"
        f"a_n_dspx={config_dict['routing_params']}&name={config_dict['name']}"
    )

    instruction_data = {
        'ios_instruction': {
            'title': "📱 Инструкция для iOS",
            'text': "Для использования VPN на iOS выполните следующие шаги:\n\n"
                   "1. Скачайте приложение Streisand из App Store\n"
                   "2. После установки откройте приложение\n"
                   "3. Нажмите на кнопку 'Подключиться' ниже\n"
                   "4. Импортируйте полученную конфигурацию\n"
                   "5. Включите VPN",
            'download_link': "https://apps.apple.com/app/id6450534064",
            'connect_link': connect_link
        },
        'android_instruction': {
            'title': "🤖 Инструкция для Android",
            'text': f"Для использования VPN на Android выполните следующие шаги:\n\n"
                   "1. Скачайте приложение Hiddify\n"
                   "2. После установки откройте приложение\n"
                   "3. Нажмите на '+' и выберите 'Импорт из буфера обмена'\n"
                   "4. Вставьте данную конфигурацию:\n\n"
                   f"<code>{config_str}</code>\n\n"
                   "5. Включите VPN",
            'download_link': "https://play.google.com/store/apps/details?id=app.hiddify.com",
            'show_connect': False
        },
        'macos_instruction': {
            'title': "🍎 Инструкция для macOS",
            'text': "Для использования VPN на macOS выполните следующие шаги:\n\n"
                   "1. Скачайте приложение Streisand\n"
                   "2. После установки откройте приложение\n"
                   "3. Нажмите на кнопку 'Подключиться' ниже\n"
                   "4. Импортируйте полученную конфигурацию\n"
                   "5. Включите VPN",
            'download_link': "https://apps.apple.com/app/id6450534064",
            'connect_link': connect_link
        },
        'windows_instruction': {
            'title': "🖥️ Инструкция для Windows",
            'text': f"Для использования VPN на Windows выполните следующие шаги:\n\n"
                   "1. Скачайте приложение Hiddify\n"
                   "2. После установки откройте приложение\n"
                   "3. Нажмите на '+' и выберите 'Импорт из буфера обмена'\n"
                   "4. Вставьте данную конфигурацию:\n\n"
                   f"<code>{config_str}</code>\n\n"
                   "5. Включите VPN",
            'download_link': "https://github.com/hiddify/hiddify-next/releases/latest/download/Hiddify-Windows-Setup-x64.exe",
            'show_connect': False
        },
    }

    try:
        data = instruction_data[callback_query.data]
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        keyboard.add(types.InlineKeyboardButton("📥 Скачать", url=data['download_link']))
        
        if data.get('show_connect', True):
            keyboard.add(types.InlineKeyboardButton("🔗 Подключиться", url=data['connect_link']))
            
        keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_instructions"))

        await callback_query.message.edit_text(
            f"<b>{data['title']}</b>\n\n{data['text']}",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        bot_logger.error(f"Error in process_instruction: {e}")
        await callback_query.answer("Произошла ошибка при генерации инструкции.")

@dp.callback_query_handler(lambda c: c.data == "back_to_instructions")
async def back_to_instructions(callback_query: types.CallbackQuery):
    instruction_keyboard = types.InlineKeyboardMarkup(row_width=2)
    instructions = [
        ("📱 iOS", "ios_instruction"),
        ("🤖 Android", "android_instruction"),
        ("🍎 macOS", "macos_instruction"),
        ("🖥️ Windows", "windows_instruction")
    ]

    for name, callback in instructions:
        instruction_keyboard.insert(types.InlineKeyboardButton(text=name, callback_data=callback))

    await callback_query.message.edit_text(
        "📚 <b>Выберите вашу операционную систему для настройки:</b>",
        reply_markup=instruction_keyboard,
        parse_mode=ParseMode.HTML
    )
    await callback_query.answer()

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)