import math


class EMA(object):
    def __init__(self, rpm, mins):
        self.alfa = 1. - math.exp(-5. / (mins * rpm))
        self.value = None

    def add(self, value):
        if self.value is None:
            self.value = value
        else:
            self.value = value * self.alfa + (1 - self.alfa) * self.value


class Metric(object):
    def __init__(self, rpm):
        self.count = 0
        self.last = 0
        self.emas = {str(i): EMA(rpm, i) for i in (1, 5, 15)}

    def on_new_time(self, secs):
        [ema.add(secs) for ema in self.emas.values()]
        self.last = secs
        self.count += 1

    def dump(self):
        r = {'m{}'.format(k): v.value for k, v in self.emas.items()}
        r['count'] = self.count
        r['last'] = self.last
        return r

    def __str__(self):
        return ','.join('{}:{}'.format(k, v.value) for k, v in self.dump().items())


__METRICS = {}


def create_metric(name, rpm):
    global __METRICS
    __METRICS[name] = Metric(rpm)
    return __METRICS[name]


def dump_metrics():
    result = {}
    for k, v in __METRICS.items():
        x = result
        for i in k.split('.'):
            if i not in x:
                x[i] = {}
            x = x[i]
        x.update(v.dump())
    return result
