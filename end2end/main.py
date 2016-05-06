import logging
import time
import yaml

from end2end import create_invocation, ValueCounter, Scheduler
from end2end.connectors.factory import load_connectors
from end2end.server import start_http_server


def start(fname, port=8080):
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Reading configuration from {}'.format(fname))
    with open(fname, 'r') as f:
        items = yaml.load(f)
        connectors = load_connectors(items.get('connectors', {}))
    if not connectors:
        raise Exception('No connectors information found in {}'.format(fname))
    counter = ValueCounter(int(time.time()))
    start_http_server(port)

    scheduler = Scheduler([create_invocation(counter, connector) for connector in connectors])
    scheduler.run()
