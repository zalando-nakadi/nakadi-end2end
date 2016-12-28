import json
import logging
import random
import string
import threading
import uuid
from contextlib import closing
from datetime import datetime, tzinfo, timedelta

import requests
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.ioloop import IOLoop

from end2end import metric
from end2end.connectors import Connector
from end2end.connectors.registry import DataToSend
from end2end.security import get_token

STREAM_SEPARATOR = '\n'.encode('UTF-8')

READ_TIMEOUT = 20

CONNECT_TIMEOUT = 1

ZERO_DELTA = timedelta(0)


class UtcTimeZone(tzinfo):
    def utcoffset(self, dt):
        return ZERO_DELTA

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO_DELTA


UTC_INSTANCE = UtcTimeZone()


def _create_event_type_description(topic):
    return {
        'name': topic,
        'owning_application': 'end2end_monitor',
        'category': 'business',
        'enrichment_strategies': ['metadata_enrichment'],
        'schema': {
            'type': 'json_schema',
            'schema': json.dumps({
                'properties': {
                    'value': {
                        'type': 'number',
                        'description': 'Value that is used for checking'
                    },
                    'instance_id': {
                        'type': 'string',
                        'description': 'Type describing instance id of this e2e checker'
                    },
                    'trash': {
                        'type': 'string',
                        'description': 'Trash to test message load'
                    },
                },
                'required': ['value', 'instance_id', 'trash'],
                'title': 'Schema for end2end monitoring',
            })
        }
    }


def stream_events(base_url, topic_name, verify, cursors_, instance_id, value_callback):
    streaming_cursors = json.loads(json.dumps(cursors_))
    thread_def = {
        'stopped': False
    }

    def _stream_internal():
        while not thread_def['stopped']:
            try:
                headers = {
                    'Accept': 'application/json',
                    'X-nakadi-cursors': json.dumps(streaming_cursors),
                    'Authorization': 'Bearer {}'.format(get_token())
                }
                headers.update(_generate_authorization())
                response = requests.get(
                    '{}/event-types/{}/events?batch_limit=1'.format(base_url, topic_name),
                    headers=headers,
                    verify=verify,
                    timeout=(1, 40),
                    stream=True)
                with closing(response) as r:
                    if r.status_code == 200:
                        for line in r.iter_lines(chunk_size=1):
                            batch = json.loads(line.decode('utf-8'))
                            update_cursors(streaming_cursors, batch['cursor'])
                            for evt in [x for x in batch.get('events', []) if x['instance_id'] == instance_id]:
                                value_callback(evt['value'])
                            if thread_def['stopped']:
                                logging.info('Breaking because thread for {} stopped'.format(topic_name))
                    else:
                        logging.error('Streaming for {} returned status code {}'.format(topic_name, r.status_code))
            except Exception as e:
                logging.error("Failed to process events", exc_info=e)

    t = threading.Thread(target=_stream_internal)
    t.setDaemon(True)
    t.start()
    return thread_def


def update_cursors(cursors_list, cursor):
    [c.update(cursor) for c in cursors_list if c['partition'] == cursor['partition']]


def _generate_authorization():
    return {'Authorization': 'Bearer {}'.format(get_token())}


def _prepare_defaults(params):
    h = params.get('headers', {})
    h.update(_generate_authorization())
    h['Accept-Encoding'] = 'gzip;q=0,deflate'
    params['headers'] = h
    if 'connect_timeout' not in params:
        params['connect_timeout'] = 5
    if 'request_timeout' not in params:
        params['request_timeout'] = 5


class RT(object):
    def __init__(self, base_url, max_clients, verify=True):
        self.http_client = CurlAsyncHTTPClient(IOLoop.instance(), max_clients=max_clients)
        self.base_url = base_url
        self.verify = verify

    def fetch(self, url, callback, **kwargs):
        kwargs['request_timeout'] = 60
        _prepare_defaults(kwargs)
        kwargs['validate_cert'] = self.verify
        return self.http_client.fetch('{}{}'.format(self.base_url, url), callback=callback, **kwargs)

    def stream(self, url, cb, complete_cb, **kwargs):
        kwargs['request_timeout'] = 60
        kwargs['validate_cert'] = self.verify
        _prepare_defaults(kwargs)

        return self.http_client.fetch(
            '{}{}'.format(self.base_url, url),
            callback=complete_cb,
            streaming_callback=cb,
            **kwargs)


def _generate_trash(trash_size):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(trash_size))


class NakadiConnector(Connector):
    def __init__(self, name, **kwargs):
        super(NakadiConnector, self).__init__(name, **kwargs)
        self.topic = kwargs['topic']
        self.receivers = int(kwargs.get('receivers', 1))
        self.initialized_receivers = [None for i in range(0, self.receivers)]
        self.r = RT(kwargs['host'], int(5 + 5 / self.interval), kwargs['verify'])
        self.instance_id = str(uuid.uuid4())
        self.trash = _generate_trash(kwargs['trash-size'])
        self.async_callbacks = {}
        self.sync_callbacks = {}
        self.init_callback = None
        self.cursors = None
        self.guard = threading.Condition()
        self.status_counter = metric.instance().create_status_counter('connector.{}.publish'.format(self.name))

    def deinitialize(self):
        if self.init_callback is not None:
            return IOLoop.instance().call_later(1, self.deinitialize)
        for t in self.initialized_receivers:
            t['stopped'] = True
        metric.instance().delete(self.status_counter)
        super(NakadiConnector, self).deinitialize()

    def initialize(self, init_callback):
        self.init_callback = init_callback

        def _ensure_event_type_exists():
            logging.debug('Checking if event type {} exists'.format(self.topic))

            def _on_event_type(r):
                if r.code == 404:
                    return _create_event_type()
                elif r.code == 200:
                    return _prepare_cursors()
                else:
                    logging.error('Failed to check for event type ({} {}), retrying'.format(r.code, r.body))
                    return _ensure_event_type_exists()

            return self.r.fetch('/event-types/{}'.format(self.topic), _on_event_type, method='GET')

        def _create_event_type():
            logging.debug('Creating event type {}'.format(self.topic))

            def _on_event_type_created(r):
                if r.code == 201:
                    logging.info('Created event type {}'.format(self.topic))
                    return _prepare_cursors()
                else:
                    logging.error('Failed to create event type {}, code: {}, message: {}, retrying'.format(
                        self.topic, r.code, r.body))
                    return _create_event_type()

            self.r.fetch(
                '/event-types',
                _on_event_type_created,
                method='POST',
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                body=json.dumps(_create_event_type_description(self.topic))
            )

        def _prepare_cursors():
            def _on_cursors_fetched(r):
                if r.code == 200:
                    self.cursors = [{
                                        'partition': p['partition'],
                                        'offset': p['newest_available_offset']
                                    } for p in json.loads(r.body.decode('UTF-8'))]

                    self.start_streaming()
                    self.init_callback = None
                    return init_callback()
                else:
                    logging.error('Failed to read partitions info for {}. Status code: {}, content: {}'.format(
                        self.topic, r.code, r.body))
                    return _prepare_cursors()

            return self.r.fetch(
                '/event-types/{}/partitions'.format(self.topic),
                _on_cursors_fetched,
                method='GET',
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            )

        _ensure_event_type_exists()

    def start_streaming(self):
        for i in range(0, self.receivers):
            self.initialized_receivers[i] = stream_events(
                self.r.base_url, self.topic, self.r.verify, self.cursors, self.instance_id, self.value_callback)

    def send_and_receive(self, data: DataToSend, use_sync: bool):
        super(NakadiConnector, self).send_and_receive(data, use_sync)
        self.register_async_callback(data.value, data.on_async_received, data.on_async_max_received)
        if use_sync:
            self.sync_callbacks[data.value] = data.on_sync_received

        def _on_event_pushed(r):
            self.status_counter.on_new_status(r.code)
            if r.code == 200:
                logging.info('successfully published event')
                data.on_data_sent()
                if use_sync:
                    return self._receive(data.value)
            else:
                logging.error('Failed to publish event to {}, status code: {}, content: {}'.format(
                    self.topic, r.code, r.body))
                self.delete_async_callback(data.value)

        self.r.fetch(
            '/event-types/{}/events'.format(self.topic),
            _on_event_pushed,
            method='POST',
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            body=json.dumps([{
                'metadata': {
                    'eid': str(uuid.uuid4()),
                    'event_type': self.topic,
                    'occurred_at': datetime.now().replace(tzinfo=UTC_INSTANCE).isoformat()
                },
                'value': data.value,
                'instance_id': self.instance_id,
                'trash': self.trash}]).encode('utf-8')
        )

    def _receive(self, value):
        attempts_left = [5]

        def _on_response(r):
            if r.code != 200:
                logging.warning('status {} and body {} while fetching for value {}'.format(r.code, r.body, value))
                return _fetch_again()
            batch = json.loads(r.body.decode('UTF-8'))
            update_cursors(self.cursors, batch['cursor'])
            for e in [x for x in batch['events'] if x['instance_id'] == self.instance_id]:
                if e['value'] in self.sync_callbacks:
                    self.sync_callbacks.pop(e['value'])()
            if value in self.sync_callbacks:
                return _fetch_again()

        def _fetch_again():
            if attempts_left[0] == 0:
                return logging.error('no attempts left to fetch value {}'.format(value))
            attempts_left[0] -= 1
            return self.r.fetch(
                '/event-types/{}/events?batch_limit=1&stream_limit=1'.format(self.topic),
                _on_response,
                request_timeout=20,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-nakadi-cursors': json.dumps(self.cursors)
                }
            )

        _fetch_again()

    def register_async_callback(self, value, async_callback, async_max_callback):
        if self.receivers:
            with self.guard:
                self.async_callbacks[value] = self.receivers, async_callback, async_max_callback

    def delete_async_callback(self, value):
        if self.receivers:
            with self.guard:
                if value in self.async_callbacks:
                    del self.async_callbacks[value]

    def value_callback(self, value):
        async_max_callback = None
        async_callback = None
        with self.guard:
            if value in self.async_callbacks:
                count, async_callback, async_max_callback = self.async_callbacks.pop(value)
                if count != self.receivers:
                    async_callback = None
                count -= 1
                if count > 0:
                    self.async_callbacks[value] = count, None, async_max_callback
                    async_max_callback = None
            else:
                logging.error('Callback for instance {} and value {} is not found'.format(self.instance_id, value))
        if async_callback:
            IOLoop.instance().add_callback(async_callback)
        if async_max_callback:
            IOLoop.instance().add_callback(async_max_callback)
