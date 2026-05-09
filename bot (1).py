"""
Pool Queue Bot
==============
Telegram-бот для управления очередью игроков в пул.

Архитектурные принципы:
- Единственный источник правды: класс GameState
- Каждый таймер привязан к session_token, а не к user_id
- Все мутации состояния — через StateManager (с asyncio.Lock)
- Notifier отправляет сообщения строго по user_id из текущего состояния
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ── Настройки ────────────────────────────────────────────────────────────────

BOT_TOKEN = "8793932137:AAG9MjNmhxsH91c2UwkQzyTgqIKtHfh0WXg"

CONFIRM_TIMEOUT = 2 * 60       # 2 минуты на подтверждение
GAME_DURATION   = 15 * 60      # 15 минут игры
EXTENSION_TIME  = 5 * 60       # +5 минут продления

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ── Модель данных ─────────────────────────────────────────────────────────────

class PlayerStatus(Enum):
    WAITING    = auto()   # в очереди
    CONFIRMING = auto()   # ожидает подтверждения (2 мин)
    PLAYING    = auto()   # играет
    EXTENDED   = auto()   # играет после продления


@dataclass
class Player:
    user_id: int
    username: str
    status: PlayerStatus = PlayerStatus.WAITING
    # session_token меняется при каждом переходе состояния.
    # Таймеры проверяют токен — устаревший таймер ничего не делает.
    session_token: str = field(default_factory=lambda: str(uuid.uuid4()))
    extended: bool = False   # уже продлевал игру?


@dataclass
class GameState:
    queue: List[Player] = field(default_factory=list)

    def find(self, user_id: int) -> Optional[Player]:
        for p in self.queue:
            if p.user_id == user_id:
                return p
        return None

    def active(self) -> Optional[Player]:
        """Первый игрок в очереди — всегда активный."""
        return self.queue[0] if self.queue else None

    def position(self, user_id: int) -> int:
        """1-based позиция в очереди, 0 если не найден."""
        for i, p in enumerate(self.queue):
            if p.user_id == user_id:
                return i + 1
        return 0


# ── StateManager ──────────────────────────────────────────────────────────────

class StateManager:
    """
    Единственное место, где меняется очередь.
    asyncio.Lock гарантирует, что два callback не изменят состояние одновременно.
    """

    def __init__(self) -> None:
        self.state = GameState()
        self._lock = asyncio.Lock()
        self._timers: Dict[str, asyncio.Task] = {}  # session_token → Task

    # ── публичные методы (все с lock) ────────────────────────────────────────

    async def join(self, user_id: int, username: str) -> Tuple[bool, str]:
        """Добавить игрока. Возвращает (успех, сообщение)."""
        async with self._lock:
            if self.state.find(user_id):
                return False, "Друг, ты в списке."
            player = Player(user_id=user_id, username=username or str(user_id))
            self.state.queue.append(player)
            pos = self.state.position(user_id)
            return True, f"Ты #{pos}."

    async def leave(self, user_id: int) -> Tuple[bool, Optional[Player]]:
        """
        Убрать игрока из очереди.
        Возвращает (успех, следующий_активный_если_был_первым).
        """
        async with self._lock:
            player = self.state.find(user_id)
            if not player:
                return False, None
            was_active = (self.state.queue[0].user_id == user_id) if self.state.queue else False
            self._cancel_timer(player.session_token)
            self.state.queue.remove(player)
            next_player = self.state.queue[0] if (was_active and self.state.queue) else None
            return True, next_player

    async def promote_to_confirming(self, bot) -> Optional[Player]:
        """
        Перевести первого игрока в CONFIRMING и запустить таймер.
        Вызывается после каждого изменения очереди, если первый — WAITING.
        """
        async with self._lock:
            return await self._maybe_promote(bot)

    async def confirm(self, user_id: int, bot) -> Tuple[str, Optional[Player]]:
        """
        Игрок подтвердил. Переводим в PLAYING, запускаем таймер 15 мин.
        Возвращает (статус, игрок).
        """
        async with self._lock:
            player = self.state.find(user_id)
            if not player:
                return "not_in_queue", None
            if player.status != PlayerStatus.CONFIRMING:
                return "wrong_status", None
            if self.state.queue[0].user_id != user_id:
                return "not_active", None

            self._cancel_timer(player.session_token)
            player.session_token = str(uuid.uuid4())
            player.status = PlayerStatus.PLAYING

            token = player.session_token
            self._timers[token] = asyncio.create_task(
                self._game_timer(user_id, token, GAME_DURATION, bot)
            )
            return "ok", player

    async def extend(self, user_id: int, bot) -> Tuple[str, Optional[Player]]:
        """Продлить игру на +5 минут (один раз)."""
        async with self._lock:
            player = self.state.find(user_id)
            if not player:
                return "not_in_queue", None
            if player.status not in (PlayerStatus.PLAYING, PlayerStatus.EXTENDED):
                return "wrong_status", None
            if player.extended:
                return "already_extended", None
            if self.state.queue[0].user_id != user_id:
                return "not_active", None

            self._cancel_timer(player.session_token)
            player.session_token = str(uuid.uuid4())
            player.status = PlayerStatus.EXTENDED
            player.extended = True

            token = player.session_token
            self._timers[token] = asyncio.create_task(
                self._game_timer(user_id, token, EXTENSION_TIME, bot)
            )
            return "ok", player

    async def timeout_confirm(self, user_id: int, token: str, bot) -> Optional[Player]:
        """
        Вызывается таймером подтверждения.
        Если токен устарел — ничего не делаем (игрок уже ушёл/подтвердил).
        """
        async with self._lock:
            player = self.state.find(user_id)
            if not player or player.session_token != token:
                return None
            if player.status != PlayerStatus.CONFIRMING:
                return None
            self.state.queue.remove(player)
            next_player = await self._maybe_promote(bot)
            return next_player

    async def timeout_game(self, user_id: int, token: str, bot) -> Optional[Player]:
        """
        Вызывается таймером игры (15 мин или 5 мин продления).
        """
        async with self._lock:
            player = self.state.find(user_id)
            if not player or player.session_token != token:
                return None
            if player.status not in (PlayerStatus.PLAYING, PlayerStatus.EXTENDED):
                return None
            self.state.queue.remove(player)
            next_player = await self._maybe_promote(bot)
            return next_player

    def snapshot(self) -> List[Player]:
        """Безопасная копия очереди для отображения."""
        return list(self.state.queue)

    # ── внутренние методы (вызываются внутри lock) ───────────────────────────

    async def _maybe_promote(self, bot) -> Optional[Player]:
        """
        Если первый игрок WAITING — перевести в CONFIRMING и запустить таймер.
        ВАЖНО: вызывается только внутри уже захваченного lock.
        """
        if not self.state.queue:
            return None
        first = self.state.queue[0]
        if first.status != PlayerStatus.WAITING:
            return None

        first.session_token = str(uuid.uuid4())
        first.status = PlayerStatus.CONFIRMING
        token = first.session_token

        self._timers[token] = asyncio.create_task(
            self._confirm_timer(first.user_id, token, bot)
        )
        return first

    def _cancel_timer(self, token: str) -> None:
        task = self._timers.pop(token, None)
        if task and not task.done():
            task.cancel()

    # ── таймеры (работают вне lock, вызывают методы с lock) ──────────────────

    async def _confirm_timer(self, user_id: int, token: str, bot) -> None:
        await asyncio.sleep(CONFIRM_TIMEOUT)
        log.info("Confirm timeout: user=%d token=%s", user_id, token[:8])
        try:
            await bot.send_message(
                user_id,
                "⏰Ты не подтвердил игру и удалён из очереди(.",
            )
        except Exception:
            pass
        next_p = await self.timeout_confirm(user_id, token, bot)
        if next_p:
            await notify_confirming(bot, next_p)
        await broadcast_queue(bot, self)

    async def _game_timer(self, user_id: int, token: str, duration: int, bot) -> None:
        await asyncio.sleep(duration)
        log.info("Game timeout: user=%d token=%s", user_id, token[:8])
        try:
            await bot.send_message(
                user_id,
                "⏰Отличная вышла игра! Отдохните, друзья, передаю ход следующему игроку.",
            )
        except Exception:
            pass
        next_p = await self.timeout_game(user_id, token, bot)
        if next_p:
            await notify_confirming(bot, next_p)
        await broadcast_queue(bot, self)


# ── Глобальный менеджер ───────────────────────────────────────────────────────

manager = StateManager()


# ── Вспомогательные функции уведомлений ──────────────────────────────────────

async def notify_confirming(bot, player: Player) -> None:
    """Отправить игроку запрос на подтверждение."""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я играю", callback_data="confirm"),
    ]])
    try:
        await bot.send_message(
            player.user_id,
            f"🎱 Твоя очередь, {player.username}!\n"
            f"У тебя есть {CONFIRM_TIMEOUT // 60} минуты, чтобы подтвердить игру.",
            reply_markup=kb,
        )
    except Exception as e:
        log.warning("Cannot notify %d: %s", player.user_id, e)


async def broadcast_queue(bot, mgr: StateManager) -> None:
    """Отправить всем в очереди актуальный список."""
    players = mgr.snapshot()
    if not players:
        return
    lines = []
    for i, p in enumerate(players):
        status_icon = {
            PlayerStatus.WAITING:    "⏳",
            PlayerStatus.CONFIRMING: "🔔",
            PlayerStatus.PLAYING:    "🎱",
            PlayerStatus.EXTENDED:   "🎱+",
        }.get(p.status, "?")
        lines.append(f"{i + 1}. {p.username}")
    text = "📋 Игроки:\n" + "\n".join(lines)
    for p in players:
        try:
            await bot.send_message(p.user_id, text)
        except Exception:
            pass


def queue_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Встать в очередь", callback_data="join"),
        InlineKeyboardButton("🚪 Выйти",            callback_data="leave"),
    ]])


def playing_keyboard(can_extend: bool) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton("🚪 Выйти из игры", callback_data="leave")]
    if can_extend:
        buttons.insert(0, InlineKeyboardButton("⏱ +5 минут", callback_data="extend"))
    return InlineKeyboardMarkup([buttons])


# ── Handlers ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🎱 Добро пожаловать в POOL&BEER!"
        "Подлейте пива, наслаждайтесь игрой!"
        "И помните: одна компания - одна играb\n",


        reply_markup=queue_keyboard(),
    )


async def cmd_queue(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    players = manager.snapshot()
    if not players:
        await update.message.reply_text("Очередь пуста.", reply_markup=queue_keyboard())
        return
    lines = []
    for i, p in enumerate(players):
        icon = {
            PlayerStatus.WAITING:    "⏳",
            PlayerStatus.CONFIRMING: "🔔",
            PlayerStatus.PLAYING:    "🎱",
            PlayerStatus.EXTENDED:   "🎱+",
        }.get(p.status, "?")
        lines.append(f"{i + 1}. {icon} {p.username}")
    await update.message.reply_text(
        "📋 Очередь:\n" + "\n".join(lines),
        reply_markup=queue_keyboard(),
    )


async def cb_join(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    ok, msg = await manager.join(user.id, user.first_name)
    await query.message.reply_text(msg)

    if ok:
        # Если только что добавили первого — попробовать сразу выдать ход
        first = await manager.promote_to_confirming(ctx.bot)
        if first:
            await notify_confirming(ctx.bot, first)
        await broadcast_queue(ctx.bot, manager)


async def cb_leave(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    ok, next_player = await manager.leave(user_id)
    if not ok:
        await query.message.reply_text("Ты не в очереди.")
        return

    await query.message.reply_text("Ты вышел из очереди.")
    if next_player:
        # Следующий игрок стал первым — продвинуть в CONFIRMING
        promoted = await manager.promote_to_confirming(ctx.bot)
        if promoted:
            await notify_confirming(ctx.bot, promoted)
    await broadcast_queue(ctx.bot, manager)


async def cb_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    status, player = await manager.confirm(user_id, ctx.bot)

    if status == "not_in_queue":
        await query.message.reply_text("Ты не в очереди.")
        return
    if status == "wrong_status":
        await query.message.reply_text("Сейчас не твоя очередь подтверждать.")
        return
    if status == "not_active":
        await query.message.reply_text("Ты не первый в очереди.")
        return

    kb = playing_keyboard(can_extend=True)
    await query.message.reply_text(
        f"✅ Отлично! У тебя {GAME_DURATION // 60} минут.\n"
        "Нажми «+5 минут» чтобы продлить (один раз), или «Выйти» когда закончишь.",
        reply_markup=kb,
    )



async def cb_extend(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    status, player = await manager.extend(user_id, ctx.bot)

    if status == "not_in_queue":
        await query.message.reply_text("Ты не в очереди.")
        return
    if status == "wrong_status":
        await query.message.reply_text("Продление доступно только во время игры.")
        return
    if status == "already_extended":
        await query.message.reply_text("Ты уже продлевал игру.")
        return
    if status == "not_active":
        await query.message.reply_text("Ты не первый в очереди.")
        return

    kb = playing_keyboard(can_extend=False)
    await query.message.reply_text(
        f"⏱ Продлено на {EXTENSION_TIME // 60} минут!",
        reply_markup=kb,
    )



# ── Запуск ────────────────────────────────────────────────────────────────────

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("queue", cmd_queue))

    app.add_handler(CallbackQueryHandler(cb_join,    pattern="^join$"))
    app.add_handler(CallbackQueryHandler(cb_leave,   pattern="^leave$"))
    app.add_handler(CallbackQueryHandler(cb_confirm, pattern="^confirm$"))
    app.add_handler(CallbackQueryHandler(cb_extend,  pattern="^extend$"))

    log.info("Bot started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
