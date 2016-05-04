import logging
import time

from blist import sortedset


class Metric(object):
    def on_new_time(self, secs):
        logging.info('NEW TIME!!!: {}'.format(secs))


class Connector(object):
    def __init__(self, name, interval):
        self.interval = interval
        self.name = name
        self.sync_metric = Metric()
        self.async_metric = Metric()

    def send_and_receive(self, value, callback):
        raise NotImplementedError('Not implemented, you must implement it')


class ValueCounter(object):
    def __init__(self, initial=0):
        self.value = initial

    def next(self):
        self.value += 1
        logging.debug('Incremented value counter to {}'.format(self.value))
        return self.value


def create_invocation(value_counter, connector):
    logging.info('Preparing invocation for connector {}'.format(connector.name))

    def invoke():
        value = value_counter.next()
        logging.info('Performing check for {}, {}'.format(connector.name, value))
        start_time = time.time()

        def _async_callback():
            logging.info('Async callback for {}, {}'.format(connector.name, value))
            connector.async_metric.on_new_time(time.time() - start_time)

        connector.send_and_receive(value, _async_callback)
        connector.sync_metric.on_new_time(time.time() - start_time)
        logging.info('Sync complete for {}, {}'.format(connector.name, value))

    return invoke, connector.interval


class Scheduler(object):
    def __init__(self, invocations):
        self.invocations = sortedset(key=lambda x: x[1])
        nextcall = int(time.time())
        for invoce_fcn, interval in invocations:
            self.invocations.add((invoce_fcn, nextcall, interval))

    def run(self):
        while True:
            fcn, nextcall, interval = self.invocations.pop()
            curtime = int(time.time())
            logging.info('Found curtime: {}, next: {}, interval: {}'.format(curtime, nextcall, interval))
            if nextcall <= curtime:
                fcn()
                self.invocations.add((fcn, nextcall + interval, interval))
            else:
                self.invocations.add((fcn, nextcall, interval))
                time.sleep(nextcall - curtime)
