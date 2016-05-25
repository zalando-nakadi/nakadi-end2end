import json

from tornado.httpserver import HTTPServer
from tornado.web import RequestHandler, Application, url

from end2end import metric
from end2end.connectors import registry
from end2end.connectors.factory import load_connectors


class MetricsHandler(RequestHandler):
    def get(self):
        return self.write(metric.instance().dump())


class HealthHandler(RequestHandler):
    def get(self):
        return self.write('OK')


class ConnectorsHandler(RequestHandler):
    def get(self):
        return self.write({c.name: c.config for c in registry.instance().items()})

    def post(self):
        registry.instance().set_items(load_connectors(json.loads(self.request.body.decode('utf-8'))))
        return self.write('OK')


def start_http_server(port):
    application = Application([
        url(r'/health', HealthHandler),
        url(r'/metrics', MetricsHandler),
        url(r'/connectors', ConnectorsHandler)
    ])
    HTTPServer(application).listen(port)
