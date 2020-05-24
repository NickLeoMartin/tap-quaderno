#!/usr/bin/env python3

import sys
import json
import argparse

import singer
from singer import metadata

from tap_quaderno.client import QuadernoClient
from tap_quaderno.discover import discover
from tap_quaderno.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'api_key',
    'start_date',
    'user_agent'
]


def do_discover(client):
    LOGGER.info('Testing authentication')
    client.retrieve_base_url()

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with QuadernoClient(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        else:
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state,
                 parsed_args.config['start_date'])
