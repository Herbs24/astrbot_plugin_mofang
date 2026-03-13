import os
import time
import random
import asyncio
import sqlite3
import aiohttp
from pathlib import Path

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger

# ─────────────────────────────────────────────
#  插件注册
# ─────────────────────────────────────────────

@register(
    "qfwy_mofang",
    "群聊自主学习记录并随机模仿",
    "1.0.0",
    "记录各群群员发送的所有文字消息和图片并保存在本地，群员聊天时概率将随机抽取某个群员发过的消息或者图片发送出来",
)
class AI66MofangPlugin(Star):

    # ──────────────────── 初始化 ────────────────────

    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config

        # 配置项（带默认值）
        self.data_dir   = Path(config.get("数据目录", "./data/AI66"))
        self.prob       = float(config.get("模仿概率", 0.12))
        self.cooldown_s = int(config.get("冷却秒", 20))
        self.max_chars  = int(config.get("最大保存字数", 200))
        self.allow_qq   = list(config.get("允许清理的QQ号", []))

        self.img_dir = self.data_dir / "images"
        self.img_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = self.data_dir / "ai66.db"
        self._init_db()

        # 每群冷却 {群号: 截止时间戳(ms)}
        self._cooldown: dict[str, float] = {}

        # 启动月度自动清理
        asyncio.create_task(self._auto_clean_loop())

        logger.info("AI66 魔方插件已加载")

    # ──────────────────── 数据库 ────────────────────

    def _init_db(self):
        """初始化 SQLite 数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ai66_message (
                    id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    群号    TEXT    NOT NULL,
                    用户    TEXT    NOT NULL,
                    内容    TEXT    DEFAULT '',
                    图片    TEXT    DEFAULT '',
                    时间    INTEGER NOT NULL,
                    权重    REAL    DEFAULT 1.0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_群号 ON ai66_message(群号)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_时间 ON ai66_message(时间)")
            conn.commit()

    def _db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ──────────────────── 清理函数 ────────────────────

    async def _clean_expired(self) -> int:
        """清理 30 天前的数据，同时删除本地图片文件"""
        one_month_ms = 1000 * 60 * 60 * 24 * 30
        cutoff = int(time.time() * 1000) - one_month_ms

        with self._db() as conn:
            rows = conn.execute(
                "SELECT id, 图片 FROM ai66_message WHERE 时间 < ?", (cutoff,)
            ).fetchall()

            for row in rows:
                img_path = row["图片"]
                if img_path and os.path.exists(img_path):
                    try:
                        os.remove(img_path)
                    except OSError:
                        pass

            if rows:
                ids = [r["id"] for r in rows]
                conn.execute(
                    f"DELETE FROM ai66_message WHERE id IN ({','.join('?' * len(ids))})",
                    ids
                )
                conn.commit()

        return len(rows)

    async def _auto_clean_loop(self):
        """每月执行一次自动清理"""
        one_month_s = 60 * 60 * 24 * 30
        while True:
            await asyncio.sleep(one_month_s)
            try:
                count = await self._clean_expired()
                if count > 0:
                    logger.info(f"[AI66] 自动清理完成：{count} 条")
            except Exception as e:
                logger.warning(f"[AI66] 自动清理出错：{e}")

    # ──────────────────── 私聊清理指令 ────────────────────

    @filter.command("ai清理")
    async def cmd_clean(self, event: AstrMessageEvent):
        """私聊指令：/ai清理  仅白名单QQ可用，且必须在私聊中使用"""

        # 必须私聊（无群号）
        if event.get_group_id():
            yield event.plain_result("❌ 该指令只能在私聊中使用")
            return

        sender_id = str(event.get_sender_id())
        if sender_id not in [str(q) for q in self.allow_qq]:
            yield event.plain_result("❌ 你没有清理权限")
            return

        count = await self._clean_expired()
        yield event.plain_result(f"🧹 清理完成\n已清理所有群中过期数据：{count} 条")

    # ──────────────────── 消息监听 ────────────────────

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        """监听群消息：学习 + 随机模仿"""

        group_id  = str(event.get_group_id() or "")
        sender_id = str(event.get_sender_id() or "")
        bot_id    = str(getattr(self.context, "bot_id", "") or "")

        if not group_id:
            return
        # 忽略机器人自身消息
        if sender_id == bot_id:
            return

        now_ms = int(time.time() * 1000)

        # ── 解析消息元素 ──
        raw_text = ""
        img_url  = ""
        has_forward = False
        has_at = False

        message_chain = event.get_messages()

        for seg in message_chain:
            seg_type = getattr(seg, "type", None) or seg.__class__.__name__.lower()

            if seg_type in ("at", "At"):
                has_at = True
                continue

            if seg_type in ("forward", "Forward"):
                has_forward = True
                continue

            if seg_type in ("text", "Text", "Plain"):
                raw_text += getattr(seg, "text", getattr(seg, "content", ""))

            if seg_type in ("image", "Image", "img", "Img"):
                if not img_url:
                    img_url = (
                        getattr(seg, "url",  None) or
                        getattr(seg, "file", None) or
                        getattr(seg, "src",  None) or ""
                    )

        # 跳过转发消息
        if has_forward:
            return

        raw_text = raw_text.strip()[: self.max_chars]
        if not raw_text and not img_url:
            return

        # ── 下载图片 ──
        img_path = ""
        if img_url:
            filename = f"{now_ms}_{random.random():.8f}".replace(".", "") + ".jpg"
            save_path = str(self.img_dir / filename)
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(img_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None, lambda: open(save_path, "wb").write(data)
                            )
                            img_path = save_path
            except Exception as e:
                logger.warning(f"[AI66] 图片下载失败：{e}")

        # ── 存入数据库（含权重计算） ──
        with self._db() as conn:
            row_count = conn.execute(
                "SELECT COUNT(*) FROM ai66_message WHERE 群号=? AND 用户=?",
                (group_id, sender_id)
            ).fetchone()[0]

            weight = 1.0 + row_count * 0.05

            conn.execute(
                "INSERT INTO ai66_message (群号,用户,内容,图片,时间,权重) VALUES (?,?,?,?,?,?)",
                (group_id, sender_id, raw_text, img_path, now_ms, weight)
            )
            conn.commit()

        # ── 判断是否触发模仿 ──
        now_s = time.time()
        cd_until = self._cooldown.get(group_id, 0)
        if now_s < cd_until:
            return

        if random.random() > self.prob:
            return

        # ── 加权随机选取历史消息 ──
        with self._db() as conn:
            all_msgs = conn.execute(
                "SELECT 内容, 图片, 权重 FROM ai66_message WHERE 群号=?",
                (group_id,)
            ).fetchall()

        if not all_msgs:
            return

        total_weight = sum(float(m["权重"] or 1) for m in all_msgs)
        rand_val = random.random() * total_weight
        chosen = None
        for m in all_msgs:
            rand_val -= float(m["权重"] or 1)
            if rand_val <= 0:
                chosen = m
                break

        if not chosen:
            chosen = all_msgs[-1]

        # ── 设置冷却 ──
        self._cooldown[group_id] = now_s + self.cooldown_s

        # ── 发送模仿内容 ──
        img_file = chosen["图片"]
        content  = chosen["内容"]

        if img_file and os.path.exists(img_file):
            yield event.image_result(img_file)
        elif content:
            yield event.plain_result(content)
