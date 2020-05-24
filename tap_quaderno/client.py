import time
from datetime import datetime, timedelta

import backoff
import requests
import singer
from singer import metrics
from requests.exceptions import ConnectionError

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


class RateLimitError(Exception):
    pass


class QuadernoClient(object):

    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__api_key = config.get('api_key')
        self.__base_url = None
        self.__session = requests.Session()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__session.close()

    def retrieve_base_url(self):
        data, headers = self.request(
            'GET',
            url='https://quadernoapp.com/api/authorization.json',
            skip_quota=True)

        self.__base_url = data['identity']['href']
        LOGGER.info(f'Quaderno account URL: {self.__base_url}')

    def sleep_for_reset_period(self, response):
        reset = datetime.fromtimestamp(
            int(response.headers['X-RateLimit-Reset']))

        # Pad for clock drift/sync issues
        sleep_time = (reset - datetime.now()).total_seconds() + 10
        warning_message = ('Sleeping for {:.2f} seconds '
                           'for next rate limit window')
        LOGGER.warn(warning_message.format(sleep_time))
        time.sleep(sleep_time)

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, RateLimitError, ConnectionError),
                          max_tries=5,
                          factor=3)
    def request(self, method, path=None, url=None, skip_quota=False, **kwargs):
        if (url is None and self.__base_url is None):
            self.retrieve_base_url()

        if url is None and path:
            url = '{}{}'.format(self.__base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        # https://developers.quaderno.io/api/#making-a-request
        kwargs['headers']['Authorization'] = f'Basic {self.__api_key}'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            LOGGER.warn('Rate limit hit - 429')
            self.sleep_for_reset_period(response)
            raise RateLimitError()

        response.raise_for_status()

        return response.json(), response.headers

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)
