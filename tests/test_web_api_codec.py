"""ShapeType payload checks for the Web API.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
"""

from dataclasses import dataclass
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from web_api_codec import (endpoint_paths, fill_object, key_names,
                           reader_replays_history, schema_for_node, to_json_value)


class Roles:
    IsStrRole = 1
    IsIntRole = 2
    IsEnumRole = 3
    IsFloatRole = 4
    IsBoolRole = 5
    IsSequenceRole = 6
    IsArrayRole = 7
    IsOptionalRole = 8
    IsSequenceElementRole = 9
    IsArrayElementRole = 10
    IsOptionalElementRole = 11
    IsStructRole = 12


class Node:
    def __init__(self, name, role, children=()):
        self.itemName = name
        self.itemTypeName = "int32" if role == Roles.IsIntRole else ""
        self.role = role
        self.childItems = list(children)
        self.enumItemNames = []
        self.maxElements = 0


@dataclass
class ShapeType:
    color: str = ""
    x: int = 0
    y: int = 0
    shapesize: int = 0

    __idl_field_annotations__ = {"color": {"key": True}}


SHAPE_NODE = Node("root", Roles.IsStructRole, (
    Node("color", Roles.IsStrRole),
    Node("x", Roles.IsIntRole),
    Node("y", Roles.IsIntRole),
    Node("shapesize", Roles.IsIntRole),
))


class ShapeApiTests(unittest.TestCase):
    def test_struct_routes_disambiguate_shapes_topics_and_domains(self):
        from types import SimpleNamespace
        item = lambda topic, domain: SimpleNamespace(topic_type="Demo::ShapeType",
                                                     topic_name=topic, domainId=domain)
        self.assertEqual(endpoint_paths([item("Circle", 0)], "readers")[0][0],
                         "/api/readers/ShapeType")
        routes = endpoint_paths([item("Circle", 0), item("Square", 0),
                                 item("Circle", 1)], "readers")
        self.assertEqual([path for path, _ in routes],
                         ["/api/readers/ShapeType/Circle/domain/0",
                          "/api/readers/ShapeType/Square",
                          "/api/readers/ShapeType/Circle/domain/1"])

        hello = lambda domain: SimpleNamespace(topic_type="Demo::HelloWorld",
                                               topic_name="HelloWorld", domainId=domain)
        self.assertEqual([path for path, _ in endpoint_paths(
            [hello(1), hello(2)], "readers")],
            ["/api/readers/HelloWorld/domain/1",
             "/api/readers/HelloWorld/domain/2"])

    def test_only_transient_readers_replay_cached_samples(self):
        for kind, expected in (("Volatile", False), ("TransientLocal", True),
                               ("Transient", True), ("Persistent", False)):
            with self.subTest(kind=kind):
                self.assertEqual(reader_replays_history({"endpoint_qos": {
                    "Durability": {"kind": kind}}}), expected)
        self.assertFalse(reader_replays_history({}))

    def test_shapes_schema_requires_only_color(self):
        schema = schema_for_node(SHAPE_NODE, Roles, key_names(ShapeType))
        self.assertEqual(schema["required"], ["color"])
        self.assertEqual(set(schema["properties"]), {"color", "x", "y", "shapesize"})

    def test_partial_circle_uses_dds_defaults(self):
        shape = fill_object(ShapeType(), {"color": "BLUE", "x": 72},
                            SHAPE_NODE, Roles, key_names(ShapeType))
        self.assertEqual(to_json_value(shape),
                         {"color": "BLUE", "x": 72, "y": 0, "shapesize": 0})

    def test_key_is_required_and_unknown_or_wrong_fields_fail(self):
        for payload in ({"x": 2}, {"color": "RED", "extra": 1},
                        {"color": "RED", "x": "2"},
                        {"color": "RED", "x": 1 << 40}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                fill_object(ShapeType(), payload, SHAPE_NODE, Roles,
                            key_names(ShapeType))

    def test_keylist_reaches_nested_field(self):
        @dataclass
        class Nested:
            name: str = ""
            count: int = 0

        @dataclass
        class Envelope:
            identity: Nested = None
            __idl_annotations__ = {"keylist": ["identity.name"]}

        node = Node("root", Roles.IsStructRole, (
            Node("identity", Roles.IsStructRole, (
                Node("name", Roles.IsStrRole), Node("count", Roles.IsIntRole))),))
        required = key_names(Envelope)
        self.assertEqual(schema_for_node(node, Roles, required)["required"], ["identity"])
        self.assertEqual(schema_for_node(node, Roles, required)["properties"]
                         ["identity"]["required"], ["name"])
        with self.assertRaisesRegex(ValueError, "identity.name"):
            fill_object(Envelope(Nested()), {"identity": {}}, node, Roles, required)


if __name__ == "__main__":
    unittest.main()
