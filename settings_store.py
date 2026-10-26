"""Guarda configurações em ~/.spark_query_studio/config.json (segredos nunca são gravados)."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from engines import AwsAccount, InstanceConfig

CONFIG_DIR = Path.home() / ".spark_query_studio"
CONFIG_FILE = CONFIG_DIR / "config.json"
SECRET_FIELDS = ("access_key", "secret_key", "session_token")


def load() -> tuple[AwsAccount, InstanceConfig]:
    if not CONFIG_FILE.exists():
        return AwsAccount(), InstanceConfig()
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        acc = AwsAccount(**{k: v for k, v in data.get("account", {}).items() if k in AwsAccount.__dataclass_fields__})
        inst = InstanceConfig(**{k: v for k, v in data.get("instance", {}).items() if k in InstanceConfig.__dataclass_fields__})
        return acc, inst
    except Exception:
        return AwsAccount(), InstanceConfig()


def save(account: AwsAccount, instance: InstanceConfig) -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    acc = asdict(account)
    for f in SECRET_FIELDS:
        acc[f] = ""
    CONFIG_FILE.write_text(json.dumps({"account": acc, "instance": asdict(instance)}, indent=2), encoding="utf-8")
    return CONFIG_FILE
