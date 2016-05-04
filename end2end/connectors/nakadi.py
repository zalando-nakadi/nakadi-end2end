import json
import logging
import random
import requests
import threading
import time
import uuid
import string
from contextlib import closing

from end2end import Connector


def update_cursors(cursors_list, cursor):
    [c.update(cursor) for c in cursors_list if c.partition == cursor.partition]


class R(object):
    def __init__(self, base_url, verify=True):
        self.base_url = base_url
        self.verify = bool(verify)

    def _update(self, kwargs):
        headers = kwargs.get('headers', {})
        headers['Authorization'] = 'Bearer: 05963b06-9802-4cde-ba8d-294f514b70a5'
        kwargs.update({
            'verify': self.verify,
            'headers': headers
        })
        return kwargs

    def get(self, url, **kwargs):
        return requests.get('{}{}'.format(self.base_url, url), **self._update(kwargs))

    def post(self, url, data, **kwargs):
        return requests.post('{}{}'.format(self.base_url, url), data, **self._update(kwargs))


def _generate_trash(trash_size):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in xrange(trash_size))


class NakadiConnector(Connector):
    def __init__(self, name, interval, **kwargs):
        super(NakadiConnector, self).__init__(name, interval)
        self.topic = kwargs['topic']
        self.r = R(kwargs['host'], kwargs['verify'])
        self.instance_id = str(uuid.uuid4())
        self.trash = _generate_trash(kwargs['trash-size'])
        self.callbacks = {}
        self._ensure_event_type_exists()
        self.cursors = self._prepare_cursors()
        threading.Thread(self._stream_events)

    def _stream_events(self):
        streaming_cursors = self._prepare_cursors()

        while True:
            response = self.r.get(
                '/event-types/{}/events'.format(self.topic),
                params={'batch_limit': 1},
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-nakadi-cursors': json.dumps(streaming_cursors)
                },
                stream=True)
            with closing(response) as r:
                if r.status_code == 200:
                    for line in r.iter_lines():
                        batch = json.loads(line)
                        update_cursors(streaming_cursors, batch.cursor)
                        for e in [e for e in batch.events if e.instance_id == self.instance_id]:
                            try:
                                self.callbacks.pop(e.value)()
                            except KeyError:
                                logging.warn('Callback for instance {} and value {} is not found'.format(
                                    self.instance_id, e.value))
                else:
                    logging.error('Streaming for {} returned code {}'.format(self.topic, r.status_code))

    def _prepare_cursors(self):
        response = self.r.get(
            '/event-types/{}/partitions'.format(self.topic),
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
        )
        if response.status_code == 200:
            return [{
                        'partition': p.partition,
                        'offset': p.newest_available_offset
                    } for p in response.json()]
        else:
            logging.error('Failed to read partitions info for {}. Status code: {}, content: {}'.format(
                self.topic, response.status_code, response.json()))
            raise Exception('Failed to read partitions info for {}. Status code: {}, content: {}'.format(
                self.topic, response.status_code, response.json()))

    def _ensure_event_type_exists(self):
        logging.debug('Checking if event type {} exists'.format(self.topic))
        response = self.r.get('/event-types/{}'.format(self.topic))
        if response.status_code == 200:
            return logging.info('Event type {} exists, continuing'.format(self.topic))
        elif response.status_code == 404:
            return self._create_event_type()
        else:
            raise Exception('Failed to check status of event type {}. Error is {}, {}'.format(
                self.topic, response.status_code, response.json()))

    def send_and_receive(self, value, callback):
        self.callbacks[value] = callback
        response = self.r.post(
            '/event-types/{}/events'.format(self.topic),
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            data=json.dumps([{'value': value, 'instance_id': self.instance_id, 'trash': self.trash}]))
        if response.status_code == 200:
            logging.info('successfully published event')
            self.receive(value)
        else:
            logging.error('Failed to publish event to {}, status code: {}, content: {}'.format(
                self.topic, response.status_code, response.json()))
            del self.callbacks[value]

    def receive(self, value):
        attempts_left = 100
        while attempts_left > 0:
            response = self.r.get(
                '/event-types/{}/events'.format(self.topic),
                params={
                    'batch_limit': 1,
                    'stream_limit': 1,
                },
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-nakadi-cursors': json.dumps(self.cursors)
                }
            )
            if response.status_code == 200:
                batch = response.json()
                update_cursors(self.cursors, batch.cursor)

                self.cursors = batch.cursor
                candidates = [e for e in batch.events if e.instance_id == self.instance_id and e.value == value]
                if candidates:
                    return logging.info('Found previously generated event with value {} for instance id {}'.format(
                        value, self.instance_id))
                else:
                    logging.warn('No candidates found among batch {} with value {} and instance id {}'.format(
                        batch, value, self.instance_id))
            else:
                logging.error('Failed to get events of type {}, status code: {}, message: {}'.format(
                    self.topic, response.status_code, response.json()))
                attempts_left -= 1
                time.sleep(1)

    def _create_event_type(self):
        logging.debug('Creating event type {}'.format(self.topic))
        response = self.r.post(
            '/event-types',
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            data=json.dumps({
                'name': self.topic,
                'owning_application': 'end2end_monitor',
                'category': 'business',
                'schema': {
                    'type': 'json_schema',
                    'title': 'Schema for end2end monitoring',
                    'schema': {
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
                        'required': ['value', 'instance_id', 'trash']
                    }
                }}))
        if response.status_code == 200:
            return logging.info('Created event type {}'.format(self.topic))
        else:
            return logging.error('Failed to create event type {}, code: {}, message: {}'.format(
                self.topic, response.status_code, response.json()))
