"""Serialization/Deserialization utils"""

import importlib
from typing import Any


def get_topic(cls: type) -> str:
    """Returns a string representing a given type"""

    return f"{cls.__module__}#{cls.__qualname__}"

def resolve_topic(topic: str) -> Any:
    """Returns an object or attribute, located by a given string topic"""

    module_name, _, attr_path = topic.partition("#")
    module = importlib.import_module(module_name)
    return resolve_attr(module, attr_path)

def resolve_attr(obj: Any, path: str) -> Any:
    """Returns an object attribute located by given strign path"""
    if not path:
        return obj
    head, _, tail = path.partition(".")
    obj = getattr(obj, head)
    return resolve_attr(obj, tail)
