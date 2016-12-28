import functools
import logging
import time
from functools import partial

from tornado.ioloop import IOLoop

from end2end import metric
from end2end.connectors import Connector


def run_once(name):
    def _tmp(func):
        def _call(self_, *args, **kwargs):
            if not hasattr(self_, '_calls'):
                self_._calls = {}
            if name not in self_._calls:
                self_._calls[name] = func(self_, *args, **kwargs)
            return self_._calls[name]

        return _call

    return _tmp


def _log_with_warning(message, timeout):
    if timeout:
        logging.warning('Timed out for {}'.format(message))
    else:
        logging.info(message)


class DataToSend(object):
    def __init__(self, value: int, connector: Connector):
        self.value = value
        self.connector = connector
        self.start_time = time.time()

    @run_once('data_sent')
    def on_data_sent(self, timeout=False):
        _log_with_warning('Send callback for {}, {}'.format(self.connector.name, self.value), timeout)
        self.connector.send_metric.on_value(time.time() - self.start_time)

    @run_once('async_received')
    def on_async_received(self, timeout=False):
        _log_with_warning('Async callback for {}, {}'.format(self.connector.name, self.value), timeout)
        self.connector.async_metric.on_value(time.time() - self.start_time)

    @run_once('async_max_received')
    def on_async_max_received(self, timeout=False):
        _log_with_warning('Async max callback for {}, {}'.format(self.connector.name, self.value), timeout)
        self.connector.async_max_metric.on_value(time.time() - self.start_time)

    @run_once('sync_received')
    def on_sync_received(self, timeout=False):
        _log_with_warning('Sync callback for {}, {}'.format(self.connector.name, self.value), timeout)
        self.connector.sync_metric.on_value(time.time() - self.start_time)

    def on_timeout_passed(self, sync_used):
        self.on_data_sent(True)
        self.on_async_received(True)
        self.on_async_max_received(True)
        if sync_used:
            self.on_sync_received(True)


class _Registry(object):
    def __init__(self):
        self._connectors = []
        self.value = int(time.time())
        self.rps = metric.instance().create_call_counter('RPS')

    def items(self):
        return tuple(self._connectors)

    def set_items(self, connectors):
        for c in self._connectors:
            c.deinitialize()
        self._connectors.clear()
        for c in connectors:
            self._connectors.append(c)
            c.initialize(partial(self._register_invocation, c))

    def _on_connector_called(self):
        pass

    def _register_invocation(self, connector):
        use_sync_calculator = connector.interval >= 2.

        def _invoke():
            if not connector.active:
                return
            self.value += 1

            data = DataToSend(self.value, connector)

            connector.send_and_receive(data, use_sync_calculator)
            IOLoop.instance().call_later(connector.interval, _invoke)
            IOLoop.instance().call_later(connector.max_wait,
                                         functools.partial(data.on_timeout_passed, use_sync_calculator))
            self.rps.on_call()

        IOLoop.instance().add_callback(_invoke)


__REGISTRY = _Registry()


def instance():
    return __REGISTRY
