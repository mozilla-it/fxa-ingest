#!/usr/bin/env python3
#
# TODO: Ability to replay events from raw_events that initially failed?
# TODO: Refactor to actually do the updates in batches (need to figure out how
#       to handle errors from multiple inserts)
# TODO: stop abusing "warning" and switch most of those to "info"
#
import random
import time, json, os, uuid, re, sys
import logging
import datetime
from threading import Lock

from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.api_core import exceptions

from fxa_ingest.utils import (fxa_source_url, parse_user_agent, unixtime_to_ts, locale_to_lang, calc_lag_seconds)

#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)

# FxA's timestamps appear to be Pacific Time
os.environ['TZ'] = 'US/Los_Angeles'
time.tzset()

logging_lock = Lock()

pubsub_project_id = os.environ.get('PUBSUB_PROJECT_ID', '')
subscription_name = os.environ.get('FXA_PUBSUB_SUBSCRIPTION_FOR_BIGQUERY', '')

bigquery_project_id = os.environ.get('BIGQUERY_PROJECT_ID', '')
bigquery_dataset    = os.environ.get('BIGQUERY_DATASET', '')
#spanner_instance_id = os.environ.get('SPANNER_INSTANCE_ID', '')
#spanner_database_id = os.environ.get('SPANNER_DATABASE_ID', 'fxa')

if not pubsub_project_id or not subscription_name or not bigquery_project_id:
    raise Exception('Required ENV vars missing')

#spanner_client   = spanner.Client(project=spanner_project_id)
#spanner_instance = spanner_client.instance(spanner_instance_id)
#spanner_database = spanner_instance.database(spanner_database_id)

bq_client  = bigquery.Client(project=bigquery_project_id)
bq_dataset = bq_client.dataset(bigquery_dataset)

bq_tables = {}
for table_name in ['raw_events', 'customer_record', 'devices', 'service_logins', 'deletes', 'primary_email_change', 'device_deletes']:
    print("getting bq table %s" % table_name)
    bq_tables[table_name] = bq_client.get_table(bq_dataset.table(table_name)) # this seems unnecessarily complicated

MAX_LANG_LENGTH    = 100
MAX_OS_LENGTH      = 100
MAX_SERVICE_LENGTH = 100
MAX_LOCALE_LENGTH  = 200
MAX_COUNTRY_LENGTH = 200
MAX_ID_LENGTH      = 500
MAX_BROWSER_LENGTH = 255
MAX_UA_LENGTH      = 1000
MAX_EMAIL_LENGTH   = 255
MAX_MC_LENGTH      = 1000

STATS      = {
    'total_messages':              0,
    'event_login':                 0,
    'event_device:create':         0,
    'event_device:delete':         0,
    'event_verified':              0,
    'event_delete':                0,
    'lag_in_seconds':              0,
    'failed_json_parse_1':         0,
    'failed_json_parse_2':         0,
    'ERROR_insert_device':         0,
    'ERROR_delete_device':         0,
    'ERROR_update_customer_email': 0,
    'ERROR_failed_rows_inserted':  0,
    'ERROR_failed_rows_failed':    0,
}
LAST_STATS = STATS.copy()

def handle_delete(event_unique_id, message_json, message_dict):
    insert_delete(event_unique_id, message_json, message_dict)
    #delete_customer_record(message_dict['uid'])
    #delete_customer_devices(message_dict['uid'])
    #delete_customer_service_logins(message_dict['uid'])
    #delete_customer_raw_events(message_dict['uid'])

def insert_delete(event_unique_id, message_json, message_dict):
    data_to_insert = {
        'insert_id':       event_unique_id,
        'fxa_id':          transform('uid', message_dict),
        'fxa_ts':          transform('ts', message_dict),
    }
    logging.debug("attemping BQ deletes insert for %s" % event_unique_id)
    response = bq_client.insert_rows(bq_tables['deletes'], [ data_to_insert ] )
    if response:
        logging.warning("deletes insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

    return True

def insert_device(event_unique_id, message_json, message_dict):
    global STATS

    data_to_insert = {
        'insert_id':       event_unique_id,
        'fxa_id':          transform('uid', message_dict),
        'fxa_ts':          transform('ts', message_dict),
        'device_id':       transform('device_id', message_dict),
        'device_type':     transform('device_type', message_dict),
        'metrics_context': transform('metrics_context', message_dict),
        'deleted':         False,
        'deleted_ts':      None,
    }

    logging.debug("attemping BQ devices insert for %s" % event_unique_id)
    response = bq_client.insert_rows(bq_tables['devices'], [ data_to_insert ] , row_ids=[ data_to_insert['device_id'] ] )
    if response:
        logging.warning("devicecs insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

    return True

def transform(field_name, data):
    if field_name == 'service':
        if 'service' in data and data['service']:
          return data['service'][:MAX_SERVICE_LENGTH]
        return None
    elif field_name == 'country':
        if 'country' in data and data['country']:
            return data['country'][:MAX_COUNTRY_LENGTH]
        return None
    elif field_name == 'country_code':
        if 'countryCode' in data and data['countryCode']:
            return data['countryCode'][:MAX_COUNTRY_LENGTH]
        return None
    elif field_name == 'newsletters':
        if 'newsletters' in data and data['newsletters']:
            return data['newsletters']
        return []
    elif field_name == 'useragent':
        return data[:MAX_UA_LENGTH]
    elif field_name == 'os':
        return data[:MAX_OS_LENGTH]
    elif field_name == 'os_ver':
        return data[:MAX_OS_LENGTH]
    elif field_name == 'browser':
        return data[:MAX_BROWSER_LENGTH]
    elif field_name == 'metrics_context':
        mc = fxa_source_url(data.get('metricsContext', {}))
        if mc:
            mc = mc[:MAX_MC_LENGTH]
        return mc
    elif field_name == 'ts':
        return unixtime_to_ts(data['ts'])
    elif field_name == 'device_type':
        return data.get('type', 'is_placeholder')
    elif field_name == 'uid':
        return data['uid'][:MAX_ID_LENGTH]
    elif field_name == 'device_id':
        return data['id'][:MAX_ID_LENGTH]
    elif field_name == 'device_count':
        return data.get('deviceCount', 0)
    elif field_name == 'email':
        if 'email' in data and data['email']:
            return data['email'][:MAX_EMAIL_LENGTH]
        else:
            return None
    elif field_name == 'marketing_opt_in':
        return bool(re.search('^(?:y(?:es)?|1|t(?:rue)?)$',
                       str(data.get('marketingOptIn', '')).lower()))
    elif field_name == 'locale':
        if 'locale' in data and data['locale']:
            return data['locale'][:MAX_LOCALE_LENGTH]
        return None
    elif field_name == 'lang':
        return locale_to_lang(data.get('locale', None))[:MAX_LANG_LENGTH]


def insert_service_login(event_unique_id, message_json, message_dict):
    logging.debug("insert_service_login called for %s" % event_unique_id)
    if 'userAgent' in message_dict:
        ua = message_dict['userAgent']
        parsed_ua = parse_user_agent(ua)
        os = parsed_ua.os.family
        os_ver = parsed_ua.os.version_string
        browser = '{0} {1}'.format(parsed_ua.browser.family,
                                   parsed_ua.browser.version_string)
    else:
        ua = ''
        os = ''
        os_ver = ''
        browser = ''

    data_to_insert = {
        'insert_id':       event_unique_id,
        'fxa_id':          transform('uid', message_dict),
        'fxa_ts':          transform('ts', message_dict),
        'service':         transform('service', message_dict),
        'device_count':    transform('device_count', message_dict),
        'country':         transform('country', message_dict),
        'country_code':    transform('country_code', message_dict),
        'useragent':       transform('useragent', ua),
        'os':              transform('os', os),
        'os_version':      transform('os_ver', os_ver),
        'browser':         transform('browser', browser),
        'metrics_context': transform('metrics_context', message_dict),
    }

    logging.debug("attemping BQ service_logins insert for %s" % event_unique_id)
    unique_id_for_bq = data_to_insert['fxa_id'] + data_to_insert['fxa_ts']
    if data_to_insert['service']:
        unique_id_for_bq += data_to_insert['service']
    response = bq_client.insert_rows(bq_tables['service_logins'], [ data_to_insert ] , row_ids=[ unique_id_for_bq ] )
    if response:
        logging.warning("insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

    return True


def delete_device(event_unique_id, message_json, message_dict):
    global STATS

    data_to_insert = {
        'insert_id': event_unique_id,
        'fxa_id':    transform('uid',       message_dict),
        'fxa_ts':    transform('ts',        message_dict),
        'device_id': transform('device_id', message_dict),
    }

    logging.debug("attemping BQ device_deletes insert for %s" % event_unique_id)
    unique_id_for_bq = data_to_insert['device_id']
    response = bq_client.insert_rows(bq_tables['device_deletes'], [ data_to_insert ] , row_ids=[ unique_id_for_bq ]  )
    if response:
        logging.warning("delete_device insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

def update_customer_email(event_unique_id, message_json, message_dict):
    global STATS

    data_to_insert = {
        'insert_id': event_unique_id,
        'fxa_id':    transform('uid',   message_dict),
        'fxa_ts':    transform('ts',    message_dict),
        'email':     transform('email', message_dict),
    }

    logging.debug("attemping BQ primary_email_change insert for %s" % event_unique_id)
    response = bq_client.insert_rows(bq_tables['primary_email_change'], [ data_to_insert ] )
    if response:
        logging.warning("primary_email_change insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

def insert_customer_record(event_unique_id, message_json, message_dict):
    logging.debug("insert_customer_record called for %s" % event_unique_id)

    data_to_insert = {
            'insert_id':         event_unique_id,
            'fxa_id':            transform('uid',              message_dict),
            'fxa_ts':            transform('ts',               message_dict),
            'email':             transform('email',            message_dict),
            'service':           transform('service',          message_dict),
            'locale':            transform('locale',           message_dict),
            'lang':              transform('lang',             message_dict),
            'marketing_opt_in':  transform('marketing_opt_in', message_dict),
            'metrics_context':   transform('metrics_context',  message_dict),
            'country':           transform('country',          message_dict),
            'country_code':      transform('country_code',     message_dict),
            'newsletters':       transform('newsletters',      message_dict),
    }

    logging.debug("attemping BQ customer insert for %s" % event_unique_id)
    #logging.info("data %s" % data_to_insert)
    # specify row_ids so BQ can dedupe on their end (we're getting dupe events from the queue)
    response = bq_client.insert_rows(bq_tables['customer_record'], [ data_to_insert ], row_ids=[ data_to_insert['fxa_id'] ] )
    if response:
        logging.warning("customer_record insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

    logging.debug("insert_customer_record finished for %s" % event_unique_id)


def insert_raw_event(event_unique_id, message_json, message_dict):
    data_to_insert = {
        'insert_id':      event_unique_id,
        'fxa_id':         transform('uid', message_dict),
        'fxa_ts':         transform('ts',  message_dict),
        'raw_event_json': message_json,
    }

    logging.debug("attemping BQ raw_events insert for %s" % event_unique_id)
    response = bq_client.insert_rows(bq_tables['raw_events'], [ data_to_insert ] )
    if response:
        logging.warning("raw_events insert error for eui: %s error(s): %s" % (event_unique_id, response))
        raise Exception("BQ insert failed")

def pubsub_callback(message):
    global STATS, LAST_STATS

    STATS['total_messages'] = 1 + STATS.get('total_messages', 0)

    logging.debug('Received message: {}'.format(message))
    message_payload = message.data.decode('utf-8')

    # generate a unique id for this event
    event_unique_id = str(uuid.uuid4())

    try:
        first_loads = json.loads(message_payload)
    except ValueError as e:
        # handle bad json here
        logging.warning("FAILED FIRST JSON PARSE")
        insert_failed_row(event_unique_id, message_payload, 'FIRST JSON PARSE', "BAD JSON")
        STATS['failed_json_parse_1'] = 1 + STATS.get('failed_json_parse_1', 0)
        message.ack()
        return

    payload_json = ''
    try:
        payload_json = first_loads['Message']
        payload_dict = json.loads(payload_json)
    except ValueError as e:
        # handle bad json here
        logging.warning("FAILED SECOND JSON PARSE")
        insert_failed_row(event_unique_id, payload_json, 'SECOND JSON PARSE', "BAD JSON")
        STATS['failed_json_parse_2'] = 1 + STATS.get('failed_json_parse_2', 0)
        message.ack()
        return

    logging.info("uid={uid}... eui={eui}... ts={ts} -- {event} event received. ".format(
        uid=payload_dict['uid'][:10],
        eui=event_unique_id[:10],
        ts=unixtime_to_ts(payload_dict['ts']),
        event=payload_dict['event'] ))

    if int(payload_dict['ts']) % 3600 == 0:
        logging.warning("processing data from {ts}".format(ts=unixtime_to_ts(payload_dict['ts'])))

    if int(datetime.datetime.now().strftime("%S"))  == 0 and logging_lock.acquire(blocking=False):
        STATS['lag_in_seconds'] = calc_lag_seconds(payload_dict['ts'])
        logging.warning("STATS: %s" % STATS)
        if LAST_STATS:
            # I'm not locking STATS, so this isn't an exact science
            per_min = {k: STATS[k] - LAST_STATS[k] for k in LAST_STATS.keys()}
            logging.warning("STATS PER MIN: %s" % per_min)
        LAST_STATS = STATS.copy()
        time.sleep(1)
        logging_lock.release()

    event = payload_dict['event']
    STATS['event_' + event] = 1 + STATS.get('event_' + event, 0)

    if event == 'delete':
        # "delete" event - remove user from all tables, including raw_events
        handle_delete(event_unique_id, payload_json, payload_dict)

    else:
        # NOT a delete event
        # pass the actual event json to insert_raw_event
        insert_raw_event(event_unique_id, payload_json, payload_dict)

        if event == 'verified':
            insert_customer_record(event_unique_id, payload_json, payload_dict)

        elif event == 'login':
            logging.debug("processing login event")
            insert_service_login(event_unique_id, payload_json, payload_dict)

        elif event == 'device:create':
            insert_device(event_unique_id, payload_json, payload_dict)

        elif event == 'device:delete':
            delete_device(event_unique_id, payload_json, payload_dict)

        elif event == 'primaryEmailChanged':
            update_customer_email(event_unique_id, payload_json, payload_dict)

        elif (event == 'profileDataChanged' or
              event == 'passwordChange' or
              event == 'reset'):
            # uncomment when historical events are loaded
            #update_customer_last_activity(event_unique_id, payload_json, payload_dict)
            pass

        else:
            print("WARNING: Unknown event '%s'" % event)
            STATS['event_UNKNOWN'] = 1 + STATS.get('event_UNKNOWN', 0)

    #logging.info(f"uid={payload_dict['uid']} eui={event_unique_id} -- {payload_dict['event']} event processed. ")
    message.ack()


def listen_loop():
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_name}`
    subscription_path = subscriber.subscription_path(
        pubsub_project_id, subscription_name)

    #flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    #subscriber.subscribe(subscription_path, callback=pubsub_callback, flow_control=flow_control)
    subscriber.subscribe(subscription_path, callback=pubsub_callback)

    # The subscriber is non-blocking. We must keep the main thread from
    # exiting to allow it to process messages asynchronously in the background.
    print('Listening for messages on {}'.format(subscription_path))
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sys.exit("\nExiting...")


if __name__ == '__main__':
#    try:
#        #create_database()
#        pass
#    except exceptions.AlreadyExists as e:
#        pass
#    for table_name in ['raw_events', 'failed_inserts', 'customer_record', 'service_logins', 'devices', 'deletes']:
#        create_table(table_name)
    listen_loop()
