import math

import time


class EMA(object):
    def __init__(self, rpm, mins):
        self.alfa = 1. - math.exp(-5. / (mins * rpm))
        self.value = None

    def add(self, value):
        if self.value is None:
            self.value = value
        else:
            self.value = value * self.alfa + (1 - self.alfa) * self.value


class Percentile(object):
    def __init__(self, percentile, data_size):
        self.percentile = percentile
        self.data_size = data_size
        self.data = []
        self.percentileValue = None

    def on_value(self, value):
        size = len(self.data)
        if size == self.data_size:
            del self.data[0]
        self.data.append(value)
        self.percentileValue = sorted(self.data)[int(math.ceil((size * self.percentile) / 100)) - 1]


class Named(object):
    def __init__(self, name):
        self.name = name


class Metric(Named):
    def __init__(self, name, rpm):
        super(Metric, self).__init__(name)
        self.count = 0
        self.last = 0
        self.emas = {str(i): EMA(rpm, i) for i in (1, 5, 15)}
        self.p95 = {str(i): Percentile(95, rpm / i) for i in (1, 5, 15)}
        self.p98 = {str(i): Percentile(98, rpm / i) for i in (1, 5, 15)}
        self.p99 = {str(i): Percentile(99, rpm / i) for i in (1, 5, 15)}

    def on_value(self, secs):
            [ema.add(secs) for ema in self.emas.values()]
            [p.on_value(secs) for p in self.p95.values()]
            [p.on_value(secs) for p in self.p98.values()]
            [p.on_value(secs) for p in self.p99.values()]
            self.last = secs
            self.count += 1

    def dump(self):
        r = {'m{}'.format(k): v.value for k, v in self.emas.items()}
        r.update({'p95_m{}'.format(k): v.percentileValue for k, v in self.p95.items()})
        r.update({'p98_m{}'.format(k): v.percentileValue for k, v in self.p98.items()})
        r.update({'p99_m{}'.format(k): v.percentileValue for k, v in self.p99.items()})
        r['count'] = self.count
        r['last'] = self.last
        return r

    def __str__(self):
        return ','.join('{}:{}'.format(k, v.value) for k, v in self.dump().items())


class CallCounter(Metric):
    def __init__(self, name):
        super(CallCounter, self).__init__(name, 60)
        self.last_update = int(time.time())
        self._counter = 0

    def _check_time(self):
        for i in range(0, int(time.time() - self.last_update)):
            self.on_value(self._counter)
            self._counter = 0
            self.last_update += 1

    def on_call(self):
        self._check_time()
        self._counter += 1


class StatusCounter(Named):
    def __init__(self, name):
        super().__init__(name)
        self.counts = {}

    def on_new_status(self, status):
        status = str(status)
        if status not in self.counts:
            self.counts[status] = 1
        else:
            self.counts[status] += 1

    def dump(self):
        return {'status_{}'.format(k): v for k, v in self.counts.items()}


class MetricsRegistry(object):
    def __init__(self):
        self._metrics = {}

    def _register(self, metric):
        self._metrics[metric.name] = metric
        return metric

    def create_metric(self, name, rpm):
        return self._register(Metric(name, rpm))

    def create_status_counter(self, name):
        return self._register(StatusCounter(name))

    def create_call_counter(self, name):
        return self._register(CallCounter(name))

    def delete(self, metric):
        if metric.name in self._metrics and self._metrics[metric.name] == metric:
            del self._metrics[metric.name]

    def dump(self):
        result = {}
        for k, v in self._metrics.items():
            x = result
            for i in k.split('.'):
                if i not in x:
                    x[i] = {}
                x = x[i]
            x.update(v.dump())
        return result


__REGISTRY = MetricsRegistry()


def instance():
    return __REGISTRY
