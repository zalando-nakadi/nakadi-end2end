import logging
import tokens

__TOKEN_PROVIDER = None


def get_token():
    if not __TOKEN_PROVIDER:
        return None
    return __TOKEN_PROVIDER()


def set_provider(provider):
    global __TOKEN_PROVIDER
    __TOKEN_PROVIDER = provider


def use_static_token(token):
    set_provider(lambda: token)


def use_berry_token(app_name):
    tokens.configure()
    tokens.manage(
        app_name,
        ['nakadi.event_stream.read', 'nakadi.event_stream.write', 'nakadi.event_type.write', 'uid'])
    tokens.start()

    def _get_token():
        try:
            return tokens.get(app_name)
        except Exception as e:
            logging.error('Failed to get token for {}'.format(app_name), exc_info=e)
            return ''

    set_provider(_get_token)
