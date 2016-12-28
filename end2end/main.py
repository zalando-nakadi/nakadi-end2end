import logging

import yaml
from tornado.ioloop import IOLoop

from end2end.connectors import registry
from end2end.connectors.factory import load_connectors
from end2end.server import start_http_server
from end2end import security


def start(fname, port, token):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s\t%(levelname)s\t%(message)s')
    logging.getLogger('tornado.curl_httpclient').setLevel(logging.WARN)
    logging.info('Reading configuration from {}'.format(fname))
    with open(fname, 'r') as f:
        items = yaml.load(f)
        connectors = load_connectors(items.get('connectors', {}))
    if not connectors:
        raise Exception('No connectors information found in {}'.format(fname))
    if token:
        security.use_static_token(token)
    else:
        security.use_berry_token('end2end_nakadi')
    start_http_server(port)
    registry.instance().set_items(connectors)
    IOLoop.instance().start()
