import os
import re
import time
import random
import asyncio
import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
import aiosqlite

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api import logger

# ─────────────────────────────────────────────
#  常量
# ─────────────────────────────────────────────

ONE_MONTH_MS = 1000 * 60 * 60 * 24 * 30
ONE_MONTH_S  = 60 * 60 * 24 * 30
DELETE_BATCH = 500       # 分批删除每批条数
SAMPLE_LIMIT = 2000      # SQL 侧采样上限，避免全量拉取

# 内网网段列表（SSRF 防护）
_PRIVATE_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
]


def _is_safe_url(url: str) -> bool:
    """校验 URL：仅允许 http/https，拒绝内网/localhost 地址"""
    if not url:
        return False
    if not re.match(r'^https?://', url, re.IGNORECASE):
        return False
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""
        ip   = ipaddress.ip_address(socket.gethostbyname(host))
        for net in _PRIVATE_NETWORKS:
            if ip in net:
                return False
    except Exception:
        return False
    return True


def _write_file(path: str, data: bytes) -> None:
    """在线程池中执行的同步写文件，with 语句保证句柄释放"""
    with open(path, "wb") as f:
        f.write(data)


# ─────────────────────────────────────────────
#  插件注册
# ─────────────────────────────────────────────

@register(
    "astrbot_plugin_qfwy-mofang",
    "群聊拟人模仿复读",
    "1.0.0",
    "",
)
class AI66MofangPlugin(Star):

    # ──────────────────── 初始化 ────────────────────

    def __init__(self, context: Context, config: dict):
        super().__init__(context)

        # 配置项
        self.prob       = float(config.get("模仿概率", 0.12))
        self.cooldown_s = int(config.get("冷却秒", 20))
        self.max_chars  = int(config.get("最大保存字数", 200))
        self.allow_qq   = [str(q) for q in config.get("允许清理的QQ号", [])]

        # ① 使用框架规范路径 StarTools.get_data_dir()
        base_dir     = Path(StarTools.get_data_dir("ai66_mofang"))
        self.img_dir = base_dir / "images"
        self.db_path = str(base_dir / "ai66.db")
        self.img_dir.mkdir(parents=True, exist_ok=True)

        # ② 线程池（仅用于同步文件 I/O）
        self._executor = ThreadPoolExecutor(max_workers=2)

        # 每群冷却 {群号: 截止时间戳(s)}
        self._cooldown: dict[str, float] = {}

        # ③ 单例 ClientSession，在 initialize() 中创建
        self._session: aiohttp.ClientSession | None = None

        # ④ 保存 task 引用，卸载时取消
        self._clean_task: asyncio.Task | None = None

        logger.info(f"插件初始化，数据目录：{base_dir}")

    # ──────────────────── 生命周期钩子 ────────────────────

    async def initialize(self):
        """插件异步初始化：建库 + 创建 session + 启动清理任务"""
        await self._init_db()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        self._clean_task = asyncio.create_task(self._auto_clean_loop())
        logger.info("异步初始化完成")

    async def destroy(self):
        """插件卸载：取消清理任务、关闭 session、关闭线程池"""
        if self._clean_task and not self._clean_task.done():
            self._clean_task.cancel()
            try:
                await self._clean_task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            await self._session.close()

        self._executor.shutdown(wait=False)
        logger.info("资源已全部释放")

    # ──────────────────── 数据库初始化 ────────────────────

    async def _init_db(self):
        """异步建表 + 建索引"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS ai66_message (
                    id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    群号 TEXT    NOT NULL,
                    用户 TEXT    NOT NULL,
                    内容 TEXT    DEFAULT '',
                    图片 TEXT    DEFAULT '',
                    时间 INTEGER NOT NULL,
                    权重 REAL    DEFAULT 1.0
                )
            """)
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_群号      ON ai66_message(群号)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_时间      ON ai66_message(时间)"
            )
            # ⑦ 补充 (群号, 用户) 复合索引，加速 COUNT 查询
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_群号_用户 ON ai66_message(群号, 用户)"
            )
            await db.commit()

    # ──────────────────── 清理函数 ────────────────────

    async def _clean_expired(self) -> int:
        """清理 30 天前数据，⑤ 分批删除避免 SQLite 变量数量限制"""
        cutoff = int(time.time() * 1000) - ONE_MONTH_MS
        total  = 0

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            while True:
                async with db.execute(
                    "SELECT id, 图片 FROM ai66_message WHERE 时间 < ? LIMIT ?",
                    (cutoff, DELETE_BATCH)
                ) as cur:
                    rows: list = list(await cur.fetchall())

                if not rows:
                    break

                for row in rows:
                    img = row["图片"]
                    if img and os.path.exists(img):
                        try:
                            os.remove(img)
                        except OSError:
                            pass

                ids = [r["id"] for r in rows]
                await db.execute(
                    f"DELETE FROM ai66_message WHERE id IN ({','.join('?'*len(ids))})",
                    ids
                )
                await db.commit()
                total += len(rows)

        return total

    async def _auto_clean_loop(self):
        """每月执行一次自动清理"""
        while True:
            await asyncio.sleep(ONE_MONTH_S)
            try:
                count = await self._clean_expired()
                if count > 0:
                    logger.info(f"[AI66] 自动清理完成：{count} 条")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[AI66] 自动清理出错：{e}")

    # ──────────────────── 私聊清理指令 ────────────────────

    @filter.command("ai清理")
    async def cmd_clean(self, event: AstrMessageEvent):
        """私聊指令：/ai清理  仅白名单QQ可用，且必须在私聊中使用"""
        if event.get_group_id():
            yield event.plain_result("❌ 该指令只能在私聊中使用")
            return

        if str(event.get_sender_id()) not in self.allow_qq:
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

        if not group_id or sender_id == bot_id:
            return

        now_ms = int(time.time() * 1000)

        # ── 解析消息（⑧ 移除无用的 has_at 变量） ──
        raw_text    = ""
        img_url     = ""
        has_forward = False

        for seg in event.get_messages():
            seg_type = getattr(seg, "type", None) or seg.__class__.__name__.lower()

            if seg_type in ("forward", "Forward"):
                has_forward = True
                break

            if seg_type in ("text", "Text", "Plain"):
                raw_text += getattr(seg, "text", getattr(seg, "content", ""))

            if seg_type in ("image", "Image", "img", "Img") and not img_url:
                img_url = (
                    getattr(seg, "url",  None) or
                    getattr(seg, "file", None) or
                    getattr(seg, "src",  None) or ""
                )

        if has_forward:
            return

        raw_text = raw_text.strip()[: self.max_chars]
        if not raw_text and not img_url:
            return

        # ── 下载图片（③ 复用 session；④ SSRF 校验） ──
        img_path = ""
        if img_url and _is_safe_url(img_url):
            filename  = f"{now_ms}_{random.getrandbits(32):08x}.jpg"
            save_path = str(self.img_dir / filename)
            try:
                async with self._session.get(img_url) as resp:  # type: ignore[union-attr]
                    if resp.status == 200:
                        data = await resp.read()
                        # ⑨ _write_file 内部用 with open 保证句柄释放
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            self._executor, _write_file, save_path, data
                        )
                        img_path = save_path
            except Exception as e:
                logger.warning(f"图片下载失败：{e}")

        # ── 异步写入数据库 ──
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM ai66_message WHERE 群号=? AND 用户=?",
                (group_id, sender_id)
            ) as cur:
                row       = await cur.fetchone()
                row_count = row[0] if row else 0

            weight = 1.0 + row_count * 0.05

            await db.execute(
                "INSERT INTO ai66_message (群号,用户,内容,图片,时间,权重) VALUES (?,?,?,?,?,?)",
                (group_id, sender_id, raw_text, img_path, now_ms, weight)
            )
            await db.commit()

        # ── 触发判断 ──
        now_s    = time.time()
        cd_until = self._cooldown.get(group_id, 0)
        if now_s < cd_until or random.random() > self.prob:
            return

        # ── ⑥ SQL 侧随机采样，避免全量拉取 ──
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT 内容, 图片, 权重
                FROM   ai66_message
                WHERE  群号 = ?
                ORDER  BY RANDOM()
                LIMIT  ?
                """,
                (group_id, SAMPLE_LIMIT)
            ) as cur:
                candidates: list = list(await cur.fetchall())


        if not candidates:
            return

        # 加权随机选取
        total_w  = sum(float(m["权重"] or 1) for m in candidates)
        rand_val = random.random() * total_w
        chosen   = candidates[-1]
        for m in candidates:
            rand_val -= float(m["权重"] or 1)
            if rand_val <= 0:
                chosen = m
                break

        # 设置冷却
        self._cooldown[group_id] = now_s + self.cooldown_s

        # 发送
        img_file = chosen["图片"]
        content  = chosen["内容"]

        if img_file and os.path.exists(img_file):
            yield event.image_result(img_file)
        elif content:
            yield event.plain_result(content)
