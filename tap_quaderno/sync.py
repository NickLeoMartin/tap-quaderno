import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_quaderno.discover import discover

LOGGER = singer.get_logger()

STEAM_CONFIGS = {
    'recurring': {
        'url_path': 'recurring.json',
        'replication': 'full'
    },
    'items': {
        'url_path': 'items.json',
        'replication': 'full'
    },
    'invoices': {
        'url_path': 'invoices.json',
        'replication': 'full'
    },
    'estimates': {
        'url_path': 'estimates.json',
        'replication': 'full'
    },
    'contacts': {
        'url_path': 'contacts.json',
        'replication': 'full'
    }
}


def get_bookmark(state, stream_name, default):
    return state.get('bookmarks', {}).get(stream_name, default)


def write_bookmark(state, stream_name, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_name] = value
    singer.write_state(state)


def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)


def process_records(stream, mdata, max_modified, records, filter_field):
    schema = stream.schema.to_dict()
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for record in records:
            record_flat = {}

            for prop, value in record.items():
                record_flat[prop] = value

            if (filter_field in record_flat
                    and record_flat[filter_field] > max_modified):
                max_modified = record_flat[filter_field]

            with Transformer() as transformer:
                record_typed = transformer.transform(record_flat,
                                                     schema,
                                                     mdata)
            singer.write_record(stream.tap_stream_id, record_typed)
            counter.increment()
        return max_modified


def sync_endpoint(client, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    stream_config = STEAM_CONFIGS[stream_name]
    filter_field = stream_config.get('filter_field')

    count = 1000
    offset = 0
    page_number = 1
    total_pages = 1
    object_limit = 25
    has_more = True
    max_modified = last_datetime
    # paginate_datetime = last_datetime

    # TODO: handle rate limiting...
    # Response details:
    # 'X-RateLimit-Reset': '14',
    # 'X-RateLimit-Remaining': '98',
    # 'X-Pages-CurrentPage': '1',
    # 'X-Pages-TotalPages': '5',
    while has_more:
        query_params = {
            'limit': object_limit,
            'page': page_number
        }

        message_template = '{} - Syncing data since {} - limit: {}, offset: {}'
        LOGGER.info(message_template.format(stream.tap_stream_id,
                                            last_datetime,
                                            count,
                                            offset))

        data, headers = client.get(
            path=stream_config['url_path'],
            params=query_params,
            endpoint=stream_name)

        records = data

        returned_total_pages = headers.get('X-Pages-TotalPages')
        if (page_number == 1 and returned_total_pages):

            total_pages = returned_total_pages
            message = (f'{stream.tap_stream_id} - '
                       f'Total pages: {total_pages}')
            LOGGER.info(message)

        max_modified = process_records(stream,
                                       mdata,
                                       max_modified,
                                       records,
                                       filter_field)

        # Exit unless more pages exist
        has_more = False
        returned_current_page = headers.get('X-Pages-CurrentPage')
        if (total_pages and returned_current_page):

            if int(returned_current_page) < int(total_pages):
                message = (
                    f'{stream.tap_stream_id} - '
                    f'Synced page {returned_current_page} of {total_pages}')
                LOGGER.info(message)

                page_number += 1
                has_more = True

        if has_more is False:
            message = (
                f'{stream.tap_stream_id} - '
                f'Completed syncing all {total_pages} pages')
            LOGGER.info(message)


def update_current_stream(state, stream_name=None):
    set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, catalog, state, start_date):
    if not catalog:
        catalog = discover()
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    selected_streams = sorted(selected_streams, key=lambda x: x.tap_stream_id)

    for stream in selected_streams:
        mdata = metadata.to_map(stream.metadata)
        update_current_stream(state, stream.tap_stream_id)
        sync_endpoint(client, catalog, state, start_date, stream, mdata)

    update_current_stream(state)
