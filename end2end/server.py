import json
import threading
import BaseHTTPServer

from metric import dump_metrics


class _RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/metrics/', '/metrics'):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(dump_metrics()))
        elif self.path in ('/health/', '/health'):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write('OK')
        else:
            self.send_response(404)
            self.end_headers()


def start_http_server(port):
    def _server_thread():
        httpd = BaseHTTPServer.HTTPServer(('0.0.0.0', port), _RequestHandler)
        httpd.serve_forever()
    t = threading.Thread(target=_server_thread)
    t.setDaemon(True)
    t.start()
