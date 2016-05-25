from end2end import metric


class Connector(object):
    def __init__(self, name, **kwargs):
        self.config = kwargs
        self.name = name
        self.interval = float(kwargs.get('interval', 10))
        if self.interval <= 0:
            self.interval = 1
        self.sync_metric = metric.instance().create_metric('connector.{}.sync'.format(name), 60. / self.interval)
        self.async_metric = metric.instance().create_metric('connector.{}.async'.format(name), 60. / self.interval)
        self.async_max_metric = metric.instance().create_metric('connector.{}.async_max'.format(name),
                                                                60. / self.interval)
        self.send_metric = metric.instance().create_metric('connector.{}.send'.format(name), 60. / self.interval)
        self.send_rpm = metric.instance().create_call_counter('connector.{}.rps'.format(name))
        self.active = True

    def send_and_receive(self, value, send_callback, sync_callback, async_callback, async_max_callback):
        self.send_rpm.on_call()

    def deinitialize(self):
        metric.instance().delete(self.sync_metric)
        metric.instance().delete(self.async_metric)
        metric.instance().delete(self.async_max_metric)
        metric.instance().delete(self.send_metric)
        metric.instance().delete(self.send_rpm)
        self.active = False
