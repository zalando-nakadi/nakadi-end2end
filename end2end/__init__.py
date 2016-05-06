import logging
import time

from collections import deque

from end2end.metric import create_metric, dump_metrics


class Connector(object):
    def __init__(self, name, interval):
        self.interval = interval
        self.name = name
        self.sync_metric = create_metric('connector.{}.sync'.format(name), 60./interval)
        self.async_metric = create_metric('connector.{}.async'.format(name), 60./interval)

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
        self.invocations = deque()
        nextcall = int(time.time())
        for invoke_fcn, interval in invocations:
            self.invocations.append((invoke_fcn, nextcall, interval))

    def _insert_invocation(self, fcn, next_call, interval):
        # It takes a lot of time to find correct implementation of tree in python.
        # So just use deque, think several iterations over array is not a problem
        idx = 0
        for _fcn, _next_call, _interval in self.invocations:
            if next_call <= _next_call:
                self.invocations.rotate(-idx)
                self.invocations.appendleft((fcn, next_call, interval))
                self.invocations.rotate(idx)
                return
            idx += 1
        self.invocations.append((fcn, next_call, interval))

    def run(self):
        while True:
            fcn, next_call, interval = self.invocations.popleft()
            cur_time = int(time.time())
            if next_call <= cur_time:
                fcn()
                self._insert_invocation(fcn, next_call+interval, interval)
            else:
                self._insert_invocation(fcn, next_call, interval)
                time.sleep(next_call - cur_time)
