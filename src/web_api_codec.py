"""JSON conversion and validation for Insight's DDS data trees.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
"""

from dataclasses import fields, is_dataclass
from collections import Counter, defaultdict
from enum import Enum
import math
import re
from urllib.parse import quote


def endpoint_paths(items, kind):
    """Give each endpoint a readable route, disambiguating shared IDL types."""
    items = list(items)
    names = [item.topic_type.replace("::", ".").rsplit(".", 1)[-1]
             for item in items]
    type_counts = Counter(names)
    topic_counts = Counter((name, item.topic_name)
                           for name, item in zip(names, items))
    domain_counts = Counter((name, item.topic_name, item.domainId)
                            for name, item in zip(names, items))
    ordinals = defaultdict(int)
    result = []
    for name, item in zip(names, items):
        path = f"/api/{kind}/{quote(name, safe='')}"
        if type_counts[name] > 1:
            if item.topic_name != name:
                path += "/" + quote(item.topic_name, safe="")
            if topic_counts[name, item.topic_name] > 1:
                path += "/domain/" + str(item.domainId)
                if domain_counts[name, item.topic_name, item.domainId] > 1:
                    key = (name, item.topic_name, item.domainId)
                    ordinals[key] += 1
                    path += "/" + str(ordinals[key])
        result.append((path, item))
    return result


def reader_replays_history(qos):
    """Only nonvolatile reader durability permits replay to a new SSE client."""
    endpoint_qos = (qos or {}).get("endpoint_qos", {})
    durability = endpoint_qos.get("Durability", {})
    return durability.get("kind") in ("TransientLocal", "Transient")


def to_json_value(value):
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {field.name: to_json_value(getattr(value, field.name)) for field in fields(value)}
    if hasattr(type(value), "__idl_typename__"):
        names = dict.fromkeys(name for cls in reversed(type(value).__mro__)
                              for name in getattr(cls, "__annotations__", {}))
        return {name: to_json_value(getattr(value, name)) for name in names}
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    if isinstance(value, bytes):
        return list(value)
    if isinstance(value, dict):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ValueError(f"Unsupported DDS value: {type(value).__name__}")


def key_names(data_type):
    """CycloneDDS stores @key on the class or individual field annotations."""
    result = set()
    for cls in reversed(getattr(data_type, "__mro__", (data_type,))):
        annotations = getattr(cls, "__idl_annotations__", {}) or {}
        keys = annotations.get("keylist", ())
        if isinstance(keys, str):
            keys = (keys,)
        result.update(keys)
        for name, annotations in (getattr(cls, "__idl_field_annotations__", {}) or {}).items():
            if annotations.get("key"):
                result.add(name)
    return result


def schema_for_node(node, roles, required=(), element_factory=None):
    role = node.role
    if role == roles.IsStrRole:
        schema = {"type": "string"}
    elif role in (roles.IsIntRole, roles.IsEnumRole):
        schema = {"type": "integer"}
        if role == roles.IsEnumRole and node.enumItemNames:
            schema["description"] = "Enum: " + ", ".join(node.enumItemNames)
    elif role == roles.IsFloatRole:
        schema = {"type": "number"}
    elif role == roles.IsBoolRole:
        schema = {"type": "boolean"}
    elif role in (roles.IsSequenceRole, roles.IsArrayRole):
        schema = {"type": "array", "items": {}}
        if node.maxElements:
            schema["maxItems"] = node.maxElements
        item_node = node.childItems[0] if node.childItems else (
            element_factory(node) if element_factory else None)
        if item_node is not None:
            schema["items"] = schema_for_node(item_node, roles, element_factory=element_factory)
        if role == roles.IsArrayRole:
            schema["minItems"] = schema["maxItems"] = len(node.childItems)
    elif role == roles.IsOptionalRole:
        schema = {"anyOf": [{"type": "null"}, {}]}
        if node.childItems:
            schema["anyOf"][1] = schema_for_node(node.childItems[0], roles,
                                                    element_factory=element_factory)
        elif element_factory:
            schema["anyOf"][1] = schema_for_node(element_factory(node), roles,
                                                    element_factory=element_factory)
    elif role in (roles.IsSequenceElementRole, roles.IsArrayElementRole,
                  roles.IsOptionalElementRole) and len(node.childItems) == 1 and not node.childItems[0].itemName:
        return schema_for_node(node.childItems[0], roles, element_factory=element_factory)
    else:
        schema = {
            "type": "object",
            "properties": {child.itemName: schema_for_node(
                child, roles,
                tuple(key[len(child.itemName) + 1:] for key in required
                      if key.startswith(child.itemName + ".")),
                element_factory=element_factory)
                           for child in node.childItems if child.itemName},
            "additionalProperties": False,
        }
        if required:
            schema["required"] = sorted({key.split(".", 1)[0] for key in required})
    return schema


def fill_object(target, payload, node, roles, required=(), path="", element_factory=None,
                element_default=None):
    """Apply a partial JSON object to a fresh DDS instance, preserving defaults."""
    if not isinstance(payload, dict):
        raise ValueError(f"{path or 'body'} must be an object")
    children = {child.itemName: child for child in node.childItems if child.itemName}
    unknown = payload.keys() - children.keys()
    if unknown:
        raise ValueError(f"Unknown field: {path}{sorted(unknown)[0]}")
    missing = {key.split(".", 1)[0] for key in required} - payload.keys()
    if missing:
        raise ValueError(f"Missing keyed field: {path}{sorted(missing)[0]}")
    for name, value in payload.items():
        child = children[name]
        current = getattr(target, name)
        nested_required = tuple(key[len(name) + 1:] for key in required
                                if key.startswith(name + "."))
        setattr(target, name, _convert(value, child, roles, current, f"{path}{name}",
                                       element_factory, element_default, nested_required))
    return target


def _convert(value, node, roles, default, path, element_factory, element_default,
             required=()):
    role = node.role
    if role == roles.IsOptionalRole and value is None:
        return None
    if role == roles.IsStrRole:
        if not isinstance(value, str):
            raise ValueError(f"{path} must be a string")
        return value
    if role in (roles.IsIntRole, roles.IsEnumRole):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{path} must be an integer")
        match = re.search(r"\b(u?)int(8|16|32|64)\b", str(node.itemTypeName))
        if match:
            bits = int(match.group(2))
            minimum = 0 if match.group(1) else -(1 << (bits - 1))
            maximum = (1 << bits) - 1 if match.group(1) else (1 << (bits - 1)) - 1
            if not minimum <= value <= maximum:
                raise ValueError(f"{path} is outside its IDL integer range")
        return value
    if role == roles.IsFloatRole:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{path} must be a number")
        if not math.isfinite(value):
            raise ValueError(f"{path} must be finite")
        return float(value)
    if role == roles.IsBoolRole:
        if not isinstance(value, bool):
            raise ValueError(f"{path} must be a boolean")
        return value
    if role in (roles.IsSequenceRole, roles.IsArrayRole):
        if not isinstance(value, list):
            raise ValueError(f"{path} must be an array")
        if node.maxElements and len(value) > node.maxElements:
            raise ValueError(f"{path} exceeds its maximum length")
        if role == roles.IsArrayRole and len(value) != len(node.childItems):
            raise ValueError(f"{path} must contain {len(node.childItems)} items")
        item_node = node.childItems[0] if node.childItems else (
            element_factory(node) if element_factory else None)
        if item_node is None and value:
            raise ValueError(f"{path} has no known element type")
        return [_convert(item, item_node, roles,
                         default[index] if default and index < len(default) else
                         element_default(node), f"{path}[{index}]",
                         element_factory, element_default)
                for index, item in enumerate(value)]
    if role == roles.IsOptionalRole:
        item_node = node.childItems[0] if node.childItems else (
            element_factory(node) if element_factory else None)
        if item_node is None:
            raise ValueError(f"{path} has no known value type")
        return _convert(value, item_node, roles, default if default is not None else
                        element_default(node), path, element_factory, element_default,
                        required)
    if role in (roles.IsSequenceElementRole, roles.IsArrayElementRole,
                roles.IsOptionalElementRole) and len(node.childItems) == 1 and not node.childItems[0].itemName:
        return _convert(value, node.childItems[0], roles, default, path,
                        element_factory, element_default, required)
    if default is None:
        raise ValueError(f"{path} has no default DDS value")
    return fill_object(default, value, node, roles, required=required, path=path + ".",
                       element_factory=element_factory, element_default=element_default)
