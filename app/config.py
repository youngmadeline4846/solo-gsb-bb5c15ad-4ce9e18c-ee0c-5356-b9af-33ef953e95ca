"""运行时配置，全部通过环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # SQLite 数据库文件路径（容器内默认 /data/alerts.db，由 Dockerfile 注入）
    database_path: str = "data/alerts.db"
    # 静默间隔（秒）：同一 source 的事件与事件段边界相距不超过该值时归入同一段
    silence_interval_seconds: float = 300.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=os.environ.get("DATABASE_PATH", "data/alerts.db"),
            silence_interval_seconds=float(
                os.environ.get("SILENCE_INTERVAL_SECONDS", "300")
            ),
        )
