from __future__ import annotations

from typing import Any, Optional
from numbers import Number
import datetime


class Constraint:
    def __init__(self, key: str, constraint_type: str, value: Optional[Any] = None):
        self.key = key
        self.constraint_type = constraint_type
        self.value = self.format_constraint_value(value)

    @staticmethod
    def format_constraint_value(value: Any) -> Any:
        if value is None:
            return
        if isinstance(value, str):
            return value
        if isinstance(value, Number):
            return str(value)
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d")

    def to_dict(self) -> dict:
        res = {
            "key": self.key,
            "constraint_type": self.constraint_type,
        }

        if self.value:
            res["value"] = self.value
        return res
