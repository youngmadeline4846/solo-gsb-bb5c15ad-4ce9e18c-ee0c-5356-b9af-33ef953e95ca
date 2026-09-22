"""告警级别定义与排序。"""
from __future__ import annotations

# 级别名称 -> 权重，数值越大越严重
_LEVELS = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "critical": 50,
}

# canonical 级别名称（按权重升序）：统计响应的固定字段次序
LEVELS = ("debug", "info", "warning", "error", "critical")

# 常见别名归一化
_ALIASES = {
    "warn": "warning",
    "fatal": "critical",
}


def normalize(severity: str) -> str:
    """归一化级别名称；非法值抛出 ValueError。"""
    name = _ALIASES.get(severity.strip().lower(), severity.strip().lower())
    if name not in _LEVELS:
        raise ValueError(
            f"未知 severity: {severity!r}，可选值: {sorted(_LEVELS)}"
        )
    return name


def level(severity: str) -> int:
    return _LEVELS[severity]


def highest(*severities: str) -> str:
    return max(severities, key=level)
