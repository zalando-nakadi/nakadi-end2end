import math


class EMA(object):
    def __init__(self, rpm, mins):
        self.alfa = 1. - math.exp(-5. * rpm / (60. * mins))
        self.value = None

    def add(self, value):
        if self.value is None:
            self.value = value
        else:
            self.value = value * self.alfa + (1 - self.alfa) * self.value


class Metric(object):
    def __init__(self, rpm):
        self.emas = {str(i): EMA(rpm, i) for i in (1, 5, 15)}

    def on_new_time(self, secs):
        [ema.add(secs) for ema in self.emas.itervalues()]

    def dump(self):
        return {'m{}'.format(k): v.value for k, v in self.emas.iteritems()}

    def __str__(self):
        return ','.join('m{}:{}'.format(k, v.value) for k, v in self.emas.iteritems())


__METRICS = {}


def create_metric(name, rpm):
    global __METRICS
    __METRICS[name] = Metric(rpm)
    return __METRICS[name]


def dump_metrics():
    result = {}
    for k, v in __METRICS.iteritems():
        x = result
        for i in k.split('.'):
            if i not in x:
                x[i] = {}
            x = x[i]
        x.update(v.dump())
    return result
