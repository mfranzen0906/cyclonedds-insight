"""Loopback HTTP interface for Insight-created readers and writers.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
"""

import json
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from PySide6.QtCore import QObject, Property, QModelIndex, QSettings, QTimer, Signal, Slot, qtTrId
from loguru import logger as logging

from models.data_tree_model import DataTreeModel, DataTreeNode
from models.tester_model import WriterItem
from web_api_codec import (endpoint_paths, fill_object, key_names,
                           reader_replays_history, schema_for_node, to_json_value)


def reject_json_constant(value):
    raise ValueError(f"Invalid JSON number: {value}")


DOCS_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>CycloneDDS Insight API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
<style>body{margin:0;font-family:sans-serif}
.insight-subscribe{margin-left:12px;padding:5px 12px;cursor:pointer}
.insight-output{margin:12px 20px;max-height:240px;overflow:auto;background:#151b23;
color:#d4e4f4;padding:12px;white-space:pre-wrap}</style>
</head><body><div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
const subscriptions=new Map();
function attachSubscriptions(){
  for(const [path, entry] of subscriptions){
    if(!document.contains(entry.button)){
      entry.source.close();entry.output.textContent='';subscriptions.delete(path);
    }
  }
  for(const operation of document.querySelectorAll('.opblock-get')){
    const summary=operation.querySelector('.opblock-summary');
    const pathElement=summary?.querySelector('.opblock-summary-path');
    const path=pathElement?.getAttribute('data-path') ||
      pathElement?.textContent.replaceAll(String.fromCharCode(8203),'').trim();
    if(!path?.startsWith('/api/readers/'))continue;
    for(const control of operation.querySelectorAll('.try-out,.execute-wrapper'))
      control.hidden=true;
    if(summary.querySelector('.insight-subscribe'))continue;
    const button=document.createElement('button');button.type='button';
    button.className='insight-subscribe';button.textContent=__SUBSCRIBE__;
    const output=document.createElement('pre');output.className='insight-output';
    output.hidden=true;summary.append(button);operation.append(output);
    button.onclick=(event)=>{
      event.stopPropagation();const current=subscriptions.get(path);
      if(current){current.source.close();subscriptions.delete(path);
        output.textContent='';output.hidden=true;
        button.textContent=__SUBSCRIBE__;return;}
      output.textContent='';output.hidden=false;
      const source=new EventSource(path);
      source.onmessage=(message)=>{
        if(subscriptions.get(path)?.source===source)
          output.textContent=message.data+'\\n'+output.textContent;
      };
      source.onerror=()=>{
        if(subscriptions.get(path)?.source===source)
          output.textContent=__RECONNECT__+'\\n'+output.textContent;
      };
      subscriptions.set(path,{source,button,output});button.textContent=__DISCONNECT__;
    };
  }
}
if(window.SwaggerUIBundle){
  SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true,
    onComplete:attachSubscriptions});
  new MutationObserver(attachSubscriptions).observe(document.querySelector('#swagger-ui'),
    {childList:true,subtree:true});
}
</script></body></html>"""


class SampleHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}
        self._subscribers = {}

    def publish(self, reader_id, sample):
        value = json.dumps(to_json_value(sample), allow_nan=False)
        with self._lock:
            self._latest[reader_id] = value
            for subscriber in self._subscribers.get(reader_id, ()):
                if subscriber.full():
                    subscriber.get_nowait()
                subscriber.put_nowait(value)

    def subscribe(self, reader_id, replay=False):
        subscriber = queue.Queue(maxsize=1)
        with self._lock:
            self._subscribers.setdefault(reader_id, set()).add(subscriber)
            latest = self._latest.get(reader_id)
            if replay and latest is not None:
                subscriber.put_nowait(latest)
        return subscriber

    def unsubscribe(self, reader_id, subscriber):
        with self._lock:
            subscribers = self._subscribers.get(reader_id)
            if subscribers is not None:
                subscribers.discard(subscriber)
                if not subscribers:
                    del self._subscribers[reader_id]

    def forget(self, reader_id):
        with self._lock:
            self._latest.pop(reader_id, None)

    def clear(self):
        with self._lock:
            self._latest.clear()


class ApiBridge(QObject):
    request = Signal(object)

    def __init__(self, listener_model, tester_model, data_handler, parent=None):
        super().__init__(parent)
        self.listener_model = listener_model
        self.tester_model = tester_model
        self.data_handler = data_handler
        self.hub = SampleHub()
        self.request.connect(self._handle_request)
        self.listener_model.rowsAboutToBeRemoved.connect(self._forget_rows)
        self.listener_model.modelAboutToBeReset.connect(self.hub.clear)
        self.listener_model.dataChanged.connect(self._forget_changed_rows)

    @Slot(QModelIndex, int, int)
    def _forget_rows(self, parent, first, last):
        reader_ids = list(self.listener_model.readers)
        for reader_id in reader_ids[first:last + 1]:
            self.hub.forget(reader_id)

    def _forget_changed_rows(self, first, last, roles):
        if (self.listener_model.StoppedRole not in roles and
                self.listener_model.TopicTypeRole not in roles):
            return
        reader_ids = list(self.listener_model.readers)
        for reader_id in reader_ids[first.row():last.row() + 1]:
            self.hub.forget(reader_id)

    def call(self, action, *args):
        completed = threading.Event()
        result = {}
        self.request.emit((action, args, result, completed))
        if not completed.wait(10):
            return 503, {"error": "Insight did not respond in time"}
        return result["value"]

    @Slot(object)
    def _handle_request(self, request):
        action, args, result, completed = request
        try:
            result["value"] = (200, self._dispatch(action, *args))
        except KeyError as error:
            result["value"] = (404, {"error": str(error.args[0])})
        except ValueError as error:
            result["value"] = (400, {"error": str(error)})
        except PermissionError as error:
            result["value"] = (409, {"error": str(error)})
        except Exception as error:
            logging.exception("Web API request failed")
            result["value"] = (500, {"error": str(error)})
        finally:
            completed.set()

    def _dispatch(self, action, *args):
        if action == "paths":
            readers = [{"name": reader.topic_type.replace("::", ".").rsplit(".", 1)[-1],
                        "domain": reader.domainId, "topic": reader.topic_name,
                        "type": reader.topic_type, "get": path,
                        "stopped": reader.stopped}
                       for path, reader in self._reader_routes()]
            writers = [{"name": item.topic_type.replace("::", ".").rsplit(".", 1)[-1],
                        "domain": item.domainId, "topic": item.topic_name,
                        "type": item.topic_type, "post": path,
                        "stopped": not item.isStarted}
                       for path, item in self._writer_routes()]
            return {"readers": readers, "writers": writers}
        if action == "docs":
            page = DOCS_HTML
            for token, message_id in (("__SUBSCRIBE__", "web_api.subscribe"),
                                      ("__DISCONNECT__", "web_api.disconnect")):
                page = page.replace(token, json.dumps(qtTrId(message_id)))
            return page.replace("__RECONNECT__", json.dumps(qtTrId("web_api.reconnect")))
        if action == "subscribe":
            reader = dict(self._reader_routes()).get(args[0])
            if reader is None:
                raise KeyError("Reader not found")
            if reader.stopped:
                raise PermissionError("Reader is stopped")
            return {"reader_id": reader.id,
                    "replay": reader_replays_history(reader.qos)}
        if action == "writer":
            path, payload = args
            item = dict(self._writer_routes()).get(path)
            if item is None:
                raise KeyError("Writer not found")
            if not item.isStarted:
                raise PermissionError("Writer is stopped")
            root = self.data_handler.getRootNode(item.topic_type)
            data_type = self.data_handler.getType(item.topic_type)
            fill_object(root.dataType, payload, root, DataTreeModel, key_names(data_type),
                        element_factory=self._element_node,
                        element_default=self._element_default)
            self.tester_model.writeDataSignal.emit(item.writerId, root.dataType)
            return {"status": "queued", "writer": path}
        if action == "openapi":
            paths = {
                "/api/paths": {"get": {"summary": qtTrId("web_api.openapi.paths"),
                    "responses": {"200": {"description": qtTrId("web_api.openapi.endpoints")}}}},
            }
            for path, reader in self._reader_routes():
                paths[path] = {"get": {"summary": qtTrId("web_api.openapi.live").replace("%1", reader.topic_name),
                    "responses": {"200": {"description": qtTrId("web_api.openapi.events"), "content": {
                        "text/event-stream": {"schema": {"type": "string"}}}}}}}
            for path, writer in self._writer_routes():
                root = self.data_handler.getRootNode(writer.topic_type)
                data_type = self.data_handler.getType(writer.topic_type)
                schema = schema_for_node(root, DataTreeModel, key_names(data_type),
                                         self._element_node)
                paths[path] = {"post": {"summary": qtTrId("web_api.openapi.write").replace("%1", writer.topic_name),
                    "requestBody": {"required": True, "content": {"application/json": {"schema": schema}}},
                    "responses": {"200": {"description": qtTrId("web_api.openapi.queued")},
                                  "400": {"description": qtTrId("web_api.openapi.invalid")}}}}
            return {"openapi": "3.0.3", "info": {"title": "CycloneDDS Insight API", "version": "1.0"},
                    "servers": [{"url": "/"}], "paths": paths}
        raise KeyError("Endpoint not found")

    def _reader_routes(self):
        return endpoint_paths(self.listener_model.readers.values(), "readers")

    def _writer_routes(self):
        return endpoint_paths((item for item in self.tester_model.items.values()
                               if isinstance(item, WriterItem)), "writers")

    def _element_node(self, parent):
        node = DataTreeNode("", parent.itemArrayTypeName,
                            DataTreeModel.IsSequenceElementRole)
        return self.data_handler.toNode(parent.itemArrayTypeName, node)

    def _element_default(self, parent):
        return self.data_handler.getInitializedDataObj(parent.itemArrayTypeName)

    @Slot(str, object)
    def on_sample(self, reader_id, sample):
        if reader_id in self.listener_model.readers:
            try:
                self.hub.publish(reader_id, sample)
            except (TypeError, ValueError) as error:
                logging.warning(f"Cannot serialize DDS sample for SSE: {error}")


def make_handler(bridge, stop_event):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _json(self, status, value):
            body = json.dumps(value, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlsplit(self.path).path
            if path in ("/", "/docs"):
                status, page = bridge.call("docs")
                if status != 200:
                    self._json(status, page)
                    return
                body = page.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/api/paths":
                status, value = bridge.call("paths")
            elif path == "/openapi.json":
                status, value = bridge.call("openapi")
            elif path.startswith("/api/readers/"):
                self._events(path)
                return
            else:
                status, value = 404, {"error": "Endpoint not found"}
            self._json(status, value)

        def do_POST(self):
            path = urlsplit(self.path).path
            if not path.startswith("/api/writers/"):
                self._json(404, {"error": "Endpoint not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > 1024 * 1024:
                    raise ValueError("Body must be between 1 byte and 1 MiB")
                if self.headers.get_content_type() != "application/json":
                    raise ValueError("Content-Type must be application/json")
                payload = json.loads(self.rfile.read(length),
                                     parse_constant=reject_json_constant)
            except (ValueError, UnicodeDecodeError) as error:
                self._json(400, {"error": str(error)})
                return
            status, value = bridge.call("writer", path, payload)
            self._json(status, value)

        def _events(self, path):
            status, value = bridge.call("subscribe", path)
            if status != 200:
                self._json(status, value)
                return
            reader_id = value["reader_id"]
            subscriber = bridge.hub.subscribe(reader_id, replay=value["replay"])
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                while not stop_event.is_set():
                    try:
                        value = subscriber.get(timeout=10)
                        self.wfile.write(f"data: {value}\n\n".encode("utf-8"))
                    except queue.Empty:
                        self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
                    status, _ = bridge.call("subscribe", path)
                    if status != 200:
                        break
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                bridge.hub.unsubscribe(reader_id, subscriber)

    return Handler


class WebApiModel(QObject):
    changed = Signal()
    operationError = Signal(str)

    def __init__(self, bridge, parent=None):
        super().__init__(parent)
        self.bridge = bridge
        self.settings = QSettings(self)
        self._enabled = self.settings.value("webApi/enabled", False, type=bool)
        self._port = self.settings.value("webApi/port", 8080, type=int)
        self._server = None
        self._thread = None
        self._stop_event = None
        if self._enabled:
            if not self._start():
                self._enabled = False

    @Property(bool, notify=changed)
    def enabled(self):
        return self._enabled

    @Property(int, notify=changed)
    def port(self):
        return self._port

    @Slot(bool)
    def setEnabled(self, enabled):
        if self._enabled == enabled:
            return
        if enabled and not self._start():
            return
        if not enabled:
            self.shutdown()
        self._enabled = enabled
        self.settings.setValue("webApi/enabled", enabled)
        self.changed.emit()

    @Slot(int)
    def setPort(self, port):
        if not 1 <= port <= 65535:
            self.operationError.emit(qtTrId("web_api.error.port"))
            return
        if self._port == port:
            return
        old_port = self._port
        was_enabled = self._enabled
        if was_enabled:
            self.shutdown()
        self._port = port
        if was_enabled and not self._start():
            self._port = old_port
            self._start()
            return
        self.settings.setValue("webApi/port", port)
        self.changed.emit()

    def _start(self):
        try:
            stop_event = threading.Event()
            server = ThreadingHTTPServer(("127.0.0.1", self._port),
                                         make_handler(self.bridge, stop_event))
            server.daemon_threads = True
            thread = threading.Thread(target=server.serve_forever, daemon=True,
                                      name="Insight Web API")
            thread.start()
            self._server, self._thread, self._stop_event = server, thread, stop_event
            logging.info(f"Web API listening at http://127.0.0.1:{self._port}/docs")
            return True
        except OSError as error:
            message = qtTrId("web_api.error.start").replace("%1", str(self._port))
            message = message.replace("%2", str(error))
            QTimer.singleShot(0, lambda: self.operationError.emit(message))
            return False

    @Slot()
    def shutdown(self):
        if self._server is not None:
            self._stop_event.set()
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=2)
            self._server = self._thread = self._stop_event = None
