"""HTTP and SSE checks using a Shapes-style sample without a DDS runtime.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
"""

from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import sys
import threading
from http.server import ThreadingHTTPServer
from types import ModuleType, SimpleNamespace
from unittest import TestCase, mock
from urllib.request import Request, urlopen


def load_http_module():
    root = Path(__file__).resolve().parents[1] / "src"
    sys.path.insert(0, str(root))
    qt = ModuleType("PySide6.QtCore")
    qt.QObject = object
    qt.QModelIndex = object
    qt.Property = qt.Slot = lambda *args, **kwargs: lambda function: function
    qt.QSettings = qt.QTimer = object
    qt.Signal = lambda *args: None
    qt.qtTrId = lambda value: value
    modules = {
        "PySide6": ModuleType("PySide6"),
        "PySide6.QtCore": qt,
        "loguru": SimpleNamespace(logger=SimpleNamespace()),
        "models.data_tree_model": SimpleNamespace(DataTreeModel=object, DataTreeNode=object),
        "models.tester_model": SimpleNamespace(WriterItem=object),
    }
    with mock.patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location("web_api_http_test", root / "web_api.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


@dataclass
class ShapeType:
    color: str
    x: int
    y: int
    shapesize: int


class FakeBridge:
    def __init__(self, module):
        self.hub = module.SampleHub()
        self.posted = []
        self.replay = False

    def call(self, action, *args):
        if action == "paths":
            return 200, {"readers": [{"name": "ShapeType", "topic": "Circle",
                                      "get": "/api/readers/ShapeType"}],
                         "writers": [{"name": "ShapeType", "post": "/api/writers/ShapeType"}]}
        if action == "subscribe" and args[0] == "/api/readers/ShapeType":
            return 200, {"reader_id": "internal-reader-1", "replay": self.replay}
        if action == "writer" and args[0] == "/api/writers/ShapeType":
            self.posted.append(args[1])
            return 200, {"status": "queued"}
        if action == "docs":
            return 200, "<html>Swagger UI</html>"
        if action == "openapi":
            return 200, {"openapi": "3.0.3", "paths": {
                "/api/readers/ShapeType": {"get": {"responses": {"200": {
                    "content": {"text/event-stream": {}}}}}}}}
        return 404, {"error": "Endpoint not found"}


class HttpApiTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_http_module()

    def setUp(self):
        self.bridge = FakeBridge(self.module)
        self.stop_event = threading.Event()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0),
                                          self.module.make_handler(self.bridge, self.stop_event))
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.stop_event.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_paths_writer_and_openapi(self):
        with urlopen(self.base + "/api/paths") as response:
            paths = json.load(response)
        self.assertEqual(paths["readers"][0]["get"], "/api/readers/ShapeType")
        with urlopen(self.base + "/openapi.json") as response:
            specification = json.load(response)
        self.assertNotIn("/openapi.json", specification["paths"])
        self.assertIn("text/event-stream", specification["paths"]
                      ["/api/readers/ShapeType"]["get"]["responses"]["200"]["content"])
        payload = {"color": "BLUE", "x": 72}
        request = Request(self.base + "/api/writers/ShapeType",
                          data=json.dumps(payload).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request) as response:
            self.assertEqual(json.load(response)["status"], "queued")
        self.assertEqual(self.bridge.posted, [payload])

    def test_bridge_uses_struct_routes_and_only_one_reader_get(self):
        reader = SimpleNamespace(id="internal-reader-1", domainId=0,
                                 topic_name="Circle", topic_type="Demo::ShapeType",
                                 qos={"endpoint_qos": {"Durability": {
                                     "kind": "TransientLocal"}}}, stopped=False)
        writer = SimpleNamespace(writerId="internal-writer-1", domainId=0,
                                 topic_name="Circle", topic_type="Demo::ShapeType",
                                 isStarted=True)
        bridge = self.module.ApiBridge.__new__(self.module.ApiBridge)
        bridge.listener_model = SimpleNamespace(readers={reader.id: reader})
        bridge.tester_model = SimpleNamespace(items={writer.writerId: writer})
        bridge.data_handler = SimpleNamespace(getRootNode=lambda name: object(),
                                              getType=lambda name: object())
        with mock.patch.object(self.module, "schema_for_node", return_value={"type": "object"}):
            paths = bridge._dispatch("paths")
            self.assertEqual(paths["readers"][0]["get"], "/api/readers/ShapeType")
            self.assertNotIn("id", paths["readers"][0])
            self.assertEqual(bridge._dispatch("subscribe", "/api/readers/ShapeType"),
                             {"reader_id": "internal-reader-1", "replay": True})
            specification = bridge._dispatch("openapi")
        self.assertNotIn("/openapi.json", specification["paths"])
        self.assertEqual(list(specification["paths"]["/api/readers/ShapeType"]),
                         ["get"])
        self.assertNotIn("/api/readers/ShapeType/events", specification["paths"])
        self.assertIn("new EventSource(path)", self.module.DOCS_HTML)
        self.assertIn(".opblock-get", self.module.DOCS_HTML)
        self.assertNotIn('<select id="reader">', self.module.DOCS_HTML)

    def test_transient_reader_replays_latest_shape(self):
        self.bridge.replay = True
        self.bridge.hub.publish("internal-reader-1", ShapeType("RED", 10, 20, 30))
        with urlopen(self.base + "/api/readers/ShapeType", timeout=3) as response:
            self.assertEqual(response.headers.get_content_type(), "text/event-stream")
            line = response.readline().decode()
            self.assertEqual(json.loads(line.removeprefix("data: ")),
                             {"color": "RED", "x": 10, "y": 20, "shapesize": 30})

    def test_volatile_reader_receives_only_future_shape(self):
        import time
        self.bridge.hub.publish("internal-reader-1", ShapeType("OLD", 1, 2, 3))
        def publish_new():
            time.sleep(0.2)
            self.bridge.hub.publish("internal-reader-1", ShapeType("NEW", 4, 5, 6))
        thread = threading.Thread(target=publish_new)
        thread.start()
        try:
            with urlopen(self.base + "/api/readers/ShapeType", timeout=3) as response:
                sample = json.loads(response.readline().decode().removeprefix("data: "))
            self.assertEqual(sample["color"], "NEW")
        finally:
            thread.join(timeout=2)
