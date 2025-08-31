import json
import logging
import os
import sys
import time
import uuid
from typing import Any

ISO = "%Y-%m-%d %H:%M:%S"


def new_run_id() -> str:
    ts = time.strftime("%Y%m%d-%H%M%S")
    return f"{ts}-{uuid.uuid4().hex[:6]}"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


class JsonLogger:
    """
    Writes newline-delimited JSON to a file AND a readable console line.
    """

    def __init__(self, out_dir: str, run_id: str, name: str = "scraper"):
        ensure_dir(out_dir)
        self.run_id = run_id
        self.path = os.path.join(out_dir, f"{run_id}.log.jsonl")

        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        self._logger.handlers.clear()

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(message)s"))
        self._logger.addHandler(ch)

        self._f = open(self.path, "a", encoding="utf-8")

    def _emit_json(self, level: str, msg: str, **kv: Any) -> None:
        evt = {
            "ts": time.strftime(ISO),
            "level": level,
            "run_id": self.run_id,
            "msg": msg,
            **kv,
        }
        self._f.write(json.dumps(evt, ensure_ascii=False) + "\n")
        self._f.flush()

    def info(self, msg: str, **kv: Any) -> None:
        self._logger.info(f"{time.strftime(ISO)} | {msg}")
        self._emit_json("INFO", msg, **kv)

    def warn(self, msg: str, **kv: Any) -> None:
        self._logger.info(f"{time.strftime(ISO)} | WARN  | {msg}")
        self._emit_json("WARN", msg, **kv)

    def error(self, msg: str, **kv: Any) -> None:
        self._logger.info(f"{time.strftime(ISO)} | ERROR | {msg}")
        self._emit_json("ERROR", msg, **kv)

    def close(self):
        try:
            self._f.close()
        except Exception:
            pass
