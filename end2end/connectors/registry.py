import logging
import time
from functools import partial

from tornado.ioloop import IOLoop

from end2end import metric


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

            value = self.value
            start_time = time.time()

            def _send_callback():
                logging.info('Send callback for {}, {}'.format(connector.name, value))
                connector.send_metric.on_value(time.time() - start_time)

            def _async_callback():
                logging.info('Async callback for {}, {}'.format(connector.name, value))
                connector.async_metric.on_value(time.time() - start_time)

            def _async_max_callback():
                logging.info('Async max callback for {}, {}'.format(connector.name, value))
                connector.async_max_metric.on_value(time.time() - start_time)

            def _sync_callback():
                logging.info('Sync callback for {}, {}'.format(connector.name, value))
                connector.sync_metric.on_value(time.time() - start_time)

            connector.send_and_receive(
                value,
                _send_callback,
                _sync_callback if use_sync_calculator else None,
                _async_callback,
                _async_max_callback
            )
            IOLoop.instance().call_later(connector.interval, _invoke)
            self.rps.on_call()

        IOLoop.instance().add_callback(_invoke)


__REGISTRY = _Registry()


def instance():
    return __REGISTRY

