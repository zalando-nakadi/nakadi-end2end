import math

import time
import os


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
    def __init__(self, sample_size):
        self.sample_size = sample_size
        self.sample = []
        self.p95 = None
        self.p98 = None
        self.p99 = None

    def add(self, value):
        self.sample.append(value)
        size = len(self.sample)
        if size == self.sample_size:
            sorted_sample = sorted(self.sample);
            self.p95 = sorted_sample[int(math.ceil((size * 95) / 100)) - 1]
            self.p98 = sorted_sample[int(math.ceil((size * 98) / 100)) - 1]
            self.p99 = sorted_sample[int(math.ceil((size * 99) / 100)) - 1]
            del self.sample[0]


class Named(object):
    def __init__(self, name):
        self.name = name


class Metric(Named):
    def __init__(self, name, rpm):
        super(Metric, self).__init__(name)
        self.count = 0
        self.last = 0
        self.emas = {str(i): EMA(rpm, i) for i in (1, 5, 15)}
        self.percentile = Percentile(os.getenv('PERCENTILE_SAMPLE_SIZE', 1000))

    def on_value(self, secs):
        [ema.add(secs) for ema in self.emas.values()]
        self.last = secs
        self.count += 1
        self.percentile.add(secs)

    def dump(self):
        r = {'m{}'.format(k): v.value for k, v in self.emas.items()}
        r['count'] = self.count
        r['last'] = self.last
        r['p95'] = self.percentile.p95
        r['p98'] = self.percentile.p98
        r['p99'] = self.percentile.p99
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
