import time
import yaml
import logging

from end2end import create_invocation, ValueCounter, Scheduler
from end2end.connectors.factory import load_connectors


def start(fname):
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Reading configuration from {}'.format(fname))
    with open(fname, 'r') as f:
        items = yaml.load(f)
        connectors = load_connectors(items.get('connectors', {}))
    if not connectors:
        raise Exception('No connectors information found in {}'.format(fname))
    counter = ValueCounter(int(time.time()))
    scheduler = Scheduler([create_invocation(counter, connector) for connector in connectors])
    scheduler.run()

