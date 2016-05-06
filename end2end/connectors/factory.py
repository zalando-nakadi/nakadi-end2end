from end2end.connectors.nakadi import NakadiConnector

CONNECTORS = {
    'nakadi': NakadiConnector
}


def __load_connector(name, spec):
    type_ = spec.get('type')
    if type_ not in CONNECTORS:
        raise Exception('Connector type {} is not supported. Supported types are: {}'.format(type_, CONNECTORS.keys()))

    return CONNECTORS[type_](name, **spec)


def load_connectors(json):
    return [__load_connector(x, y) for x, y in json.items()]
