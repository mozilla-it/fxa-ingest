#!/usr/bin/env python3
#
# TODO: Ability to replay events from raw_events that initially failed?
# TODO: Refactor to actually do the updates in batches (need to figure out how
#       to handle errors from multiple inserts)
#
import random
import time, json, os, uuid, re, sys
import logging
from google.cloud import pubsub_v1
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.api_core import exceptions

from fxa_ingest.utils import (fxa_source_url, parse_user_agent, unixtime_to_ts, locale_to_lang)
#from utils import (fxa_source_url, parse_user_agent, unixtime_to_ts, locale_to_lang)

#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)

pubsub_project_id = os.environ.get('PUBSUB_PROJECT_ID', '')
subscription_name = os.environ.get('FXA_PUBSUB_SUBSCRIPTION', '')

spanner_project_id  = os.environ.get('SPANNER_PROJECT_ID', '')
spanner_instance_id = os.environ.get('SPANNER_INSTANCE_ID', '')
spanner_database_id = os.environ.get('SPANNER_DATABASE_ID', 'fxa')

if not pubsub_project_id or not subscription_name or not spanner_project_id or not spanner_instance_id:
    raise Exception('Required ENV vars missing')

spanner_client   = spanner.Client(project=spanner_project_id)
spanner_instance = spanner_client.instance(spanner_instance_id)
spanner_database = spanner_instance.database(spanner_database_id)

MAX_OS_LENGTH      = 50
MAX_SERVICE_LENGTH = 50
MAX_LOCALE_LENGTH  = 100
MAX_COUNTRY_LENGTH = 100
MAX_ID_LENGTH      = 128
MAX_BROWSER_LENGTH = 155
MAX_UA_LENGTH      = 255
MAX_EMAIL_LENGTH   = 255
MAX_MC_LENGTH      = 255

# noinspection SqlNoDataSourceInspection
raw_events_table_ddl = ("\n"
                        "          CREATE TABLE raw_events (\n"
                       f"            insert_id        STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                        "            insert_ts        TIMESTAMP NOT NULL\n"
                        "            OPTIONS(allow_commit_timestamp=true),\n"
                       f"            fxa_id           STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                        "            fxa_ts           TIMESTAMP NOT NULL,\n"
                        "            raw_event_json   STRING(MAX)\n"
                        "          ) PRIMARY KEY (fxa_id, fxa_ts, insert_id)")

# noinspection SqlNoDataSourceInspection
failed_inserts_table_ddl = ("\n"
                            "          CREATE TABLE failed_inserts (\n"
                           f"            insert_id        STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                            "            table_name       STRING(30),\n"
                            "            error_string     STRING(MAX),\n"
                            "            failed_data      STRING(MAX),\n"
                            "            insert_ts        TIMESTAMP NOT NULL\n"
                            "            OPTIONS(allow_commit_timestamp=true)\n"
                            "          ) PRIMARY KEY (insert_ts, insert_id)")

# noinspection SqlNoDataSourceInspection
customer_record_table_ddl: str = ("\n"
                                  "CREATE TABLE customer_record (\n"
                                 f"  insert_id         STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                                 f"  fxa_id            STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                                  "  fxa_ts            TIMESTAMP NOT NULL,\n"
                                 f"  email             STRING({MAX_EMAIL_LENGTH}),\n"
                                 f"  service           STRING({MAX_SERVICE_LENGTH}),\n"
                                  "  create_ts         TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp=true),\n"
                                 f"  locale            STRING({MAX_LOCALE_LENGTH}),\n"
                                  "  lang              STRING(10),\n"
                                  "  marketing_opt_in  BOOL,\n"
                                 f"  metrics_context   STRING({MAX_MC_LENGTH})\n"
                                  ") PRIMARY KEY (fxa_id)")

# noinspection SqlNoDataSourceInspection
service_logins_table_ddl: str = ("\n"
                            "          CREATE TABLE service_logins (\n"
                           f"            insert_id        STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                           f"            fxa_id           STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                            "            fxa_ts           TIMESTAMP NOT NULL,\n"
                           f"            service          STRING({MAX_SERVICE_LENGTH}),\n"
                            "            device_count     INT64,\n"
                           f"            country          STRING({MAX_COUNTRY_LENGTH}),\n"
                           f"            useragent        STRING({MAX_UA_LENGTH}),\n"
                           f"            os               STRING({MAX_OS_LENGTH}),\n"
                           f"            os_version       STRING({MAX_OS_LENGTH}),\n"
                           f"            browser          STRING({MAX_BROWSER_LENGTH}),\n"
                           f"            metrics_context  STRING({MAX_MC_LENGTH})\n"
                            "          ) PRIMARY KEY (fxa_id, insert_id)")

# noinspection SqlNoDataSourceInspection
devices_table_ddl = ("\n"
                     "CREATE TABLE devices (\n"
                    f"  insert_id        STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                    f"  fxa_id           STRING({MAX_ID_LENGTH}) NOT NULL,\n"
                     "  fxa_ts           TIMESTAMP NOT NULL,\n"
                    f"  device_id        STRING({MAX_ID_LENGTH}),\n"
                     "  device_type      STRING(20),\n"
                    f"  metrics_context  STRING({MAX_MC_LENGTH}),\n"
                     "  deleted          BOOL,\n"
                     "  deleted_ts       TIMESTAMP\n"
                     ") PRIMARY KEY (fxa_id, device_id)")


def create_database():
    spanner_database = spanner_instance.database(spanner_database_id, ddl_statements=[
        raw_events_table_ddl,
        failed_inserts_table_ddl,
        customer_record_table_ddl,
        service_logins_table_ddl,
        devices_table_ddl,
    ])

    operation = spanner_database.create()

    print('Waiting for operation to complete...')
    operation.result()

    print('Created database {} on instance {}'.format(
        spanner_database_id, spanner_instance_id))


def create_table(table_name):
    table_def = ''
    if table_name == 'raw_events':
        table_def = raw_events_table_ddl
    elif table_name == 'failed_inserts':
        table_def = failed_inserts_table_ddl
    elif table_name == 'customer_record':
        table_def = customer_record_table_ddl
    elif table_name == 'service_logins':
        table_def = service_logins_table_ddl
    elif table_name == 'devices':
        table_def = devices_table_ddl

    try:
        operation = spanner_database.update_ddl([table_def])
        print('Waiting for operation to complete...')
        operation.result()
        print('Created {} table on database {} on instance {}'.format(
            table_name, spanner_database_id, spanner_instance_id))
    except exceptions.GoogleAPICallError as e:
        if re.search('Duplicate name in schema', str(e)):
            pass
        else:
            raise


def safe_batch_insert2(event_unique_id: str, table_name: str, data: dict) -> object:
    """

    :rtype: object
    """
    logging.debug("safe_batch_insert2 called for %s" % event_unique_id)

    columns = tuple(data.keys())
    values = tuple(data.values())

    MAX_RETRIES = 2
    for _ in range(MAX_RETRIES):
        try:
            with spanner_database.batch() as batch:
                batch.insert(
                    table=table_name,
                    columns=columns,
                    values=[values]
                )
            logging.debug("safe_batch_insert2 %s - insert succeeded ... ?" % event_unique_id)

        except exceptions.AlreadyExists as e:
            logging.debug("safe_batch_insert2 %s - exceptions.AlreadyExists encountered." % event_unique_id)
            # there's already a record, so update it. assumes the previous record was a stub
            if table_name == 'customer_record' and re.search(
                    'Row.*in table customer_record already exists', str(e)):
                logging.debug("safe_batch_insert2 %s - updating customer_record" % event_unique_id)
                update_customer_record(event_unique_id, '', data)
                break
            else:
                raise
        except Exception as e:
            logging.warning(repr(e))
            logging.warning("safe_batch_insert2 %s - some other exception found: %s" % (event_unique_id, str(e)))
            if table_name == 'failed_inserts':
                logging.error("ERROR: FAILED INSERTING!")
                logging.error("ERROR: event_unique_id: %s" % str(event_unique_id))
                logging.error("ERROR: error          : %s" % str(e))
                logging.error("ERROR: table_name     : %s" % table_name)
                logging.error("ERROR: columns        : %s" % str(columns))
                logging.error("ERROR: values         : %s" % str(values))
            else:
                insert_failed_row(event_unique_id, str(values), table_name, "Some sort of insert failure: %s" % str(e))
            break
        else:
            break



def insert_failed_row(event_unique_id: str, message: str, table: str, error_str: str) -> object:
    logging.warning("WARNING: INSERTING A FAILED ROW")
    logging.warning(f"WARNING: event_unique_id: {str(event_unique_id)}")
    logging.warning(f"WARNING: error          : {str(error_str)}")
    logging.warning(f"WARNING: table_name     : {table}")
    logging.warning(f"WARNING: message        : {str(message)}")
    safe_batch_insert2(
        event_unique_id, 'failed_inserts',
        {
            'insert_id':    event_unique_id,
            'table_name':   table,
            'error_string': error_str,
            'failed_data':  message,
            'insert_ts':    spanner.COMMIT_TIMESTAMP,
        })


def delete_customer_record(fxa_id: str) -> object:
    customer_to_delete = spanner.KeySet(keys=[[fxa_id]])
    with spanner_database.batch() as batch:
        batch.delete('customer_record', customer_to_delete)
    return True


def delete_customer_raw_events(fxa_id):
    def delete_customer(transaction):
        row_ct = transaction.execute_update(
            "DELETE raw_events WHERE fxa_id = @fxa_id",
            params={'fxa_id': fxa_id}, param_types={'fxa_id': spanner.param_types.STRING}
        )

        logging.info("{} record(s) deleted.".format(row_ct))

    spanner_database.run_in_transaction(delete_customer)

def delete_customer_devices(fxa_id):
    def delete_customer(transaction):
        row_ct = transaction.execute_update(
            "DELETE devices WHERE fxa_id = @fxa_id",
            params={'fxa_id': fxa_id}, param_types={'fxa_id': spanner.param_types.STRING}
        )

        logging.info("{} record(s) deleted.".format(row_ct))

    spanner_database.run_in_transaction(delete_customer)

def delete_customer_service_logins(fxa_id):
    def delete_customer(transaction):
        row_ct = transaction.execute_update(
            "DELETE service_logins WHERE fxa_id = @fxa_id",
            params={'fxa_id': fxa_id}, param_types={'fxa_id': spanner.param_types.STRING}
        )

        logging.info("{} record(s) deleted.".format(row_ct))

    spanner_database.run_in_transaction(delete_customer)

def handle_delete(event_unique_id, message_json, message_dict):
    # The table relationships mean all this customer's entries from
    # child tables (like devices and service_logins) will also be
    # deleted.
    #
    delete_customer_record(message_dict['uid'])
    delete_customer_devices(message_dict['uid'])
    delete_customer_service_logins(message_dict['uid'])
    delete_customer_raw_events(message_dict['uid'])
    # pass

def insert_device(event_unique_id, message_json, message_dict):
    try:
        safe_batch_insert2(
            event_unique_id, 'devices',
            {
                'insert_id':       event_unique_id,
                'fxa_id':          transform('uid', message_dict),
                'fxa_ts':          transform('ts', message_dict),
                'device_id':       transform('device_id', message_dict),
                'device_type':     transform('device_type', message_dict),
                'metrics_context': transform('metrics_context', message_dict),
                'deleted':         False,
            })
    except exceptions.AlreadyExists as e:
        logging.debug("insert_device %s - exceptions.AlreadyExists encountered. skipping insert" % event_unique_id)
        #insert_failed_row(event_unique_id, message_json, 'devices', "%s" % str(e))
        return False
    except KeyError as e:
        insert_failed_row(event_unique_id, message_json, 'devices', "Missing key: %s" % str(e))
        return False

    return True

def transform(field_name:str, data):
    if field_name == 'service':
        if 'service' in data and data['service']:
          return data['service'][:MAX_SERVICE_LENGTH]
        return None
    elif field_name == 'country':
        if 'country' in data and data['country']:
            return data['country'][:MAX_COUNTRY_LENGTH]
        return None
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
    elif field_name == 'device_county':
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
        return locale_to_lang(data.get('locale', None))


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

    try:
        safe_batch_insert2(
            event_unique_id, 'service_logins',
            {
                'insert_id':       event_unique_id,
                'fxa_id':          transform('uid', message_dict),
                'fxa_ts':          transform('ts', message_dict),
                'service':         transform('service', message_dict),
                'country':         transform('country', message_dict),
                'device_count':    transform('device_count', message_dict),
                'useragent':       transform('useragent', ua),
                'os':              transform('os', os),
                'os_version':      transform('os_ver', os_ver),
                'browser':         transform('browser', browser),
                'metrics_context': transform('metrics_context', message_dict),
            })
    except KeyError as e:
        insert_failed_row(event_unique_id, message_json, 'service_logins', "Missing key: %s" % str(e))
        return False

    return True


def delete_device(event_unique_id, message_json, message_dict):
    try:
        with spanner_database.batch() as batch:
            batch.update(
                table='devices',
                columns=('fxa_id', 'device_id', 'deleted', 'deleted_ts'),
                values=[(
                    transform('uid', message_dict),
                    transform('device_id', message_dict),
                    True, transform('ts', message_dict)
                )]
            )
    except exceptions.NotFound as e:
        logging.debug(f"delete_device failed. no record found in database. uid={message_dict['uid']} eui={event_unique_id}")


def update_customer_email(event_unique_id, message_json, message_dict):
    try:
        with spanner_database.batch() as batch:
            batch.update(
                table='customer_record',
                columns=('fxa_id', 'email'),
                values=[(
                    transform('uid',   message_dict),
                    transform('email', message_dict),
                )]
            )
    except exceptions.NotFound as e:
        logging.debug(f"update_customer_email failed. no record found in database. uid={message_dict['uid']} eui={event_unique_id}")

def update_customer_record(event_unique_id: str, message_json: str, data: dict):
    logging.debug("update_customer_record called for %s" % event_unique_id)
    with spanner_database.batch() as batch:
        batch.update(
            table='customer_record',
            columns=('insert_id', 'fxa_id', 'fxa_ts', 'email', 'service', 'create_ts',
                     'locale', 'lang', 'marketing_opt_in', 'metrics_context'),
            values=[(
                event_unique_id,
                data['fxa_id'],
                data['fxa_ts'],
                transform('email', data),
                transform('service', data),
                data['fxa_ts'],
                transform('locale', data),
                transform('lang', data),
                transform('marketing_opt_in', data),
                transform('metrics_context', data),
             )]
         )


def insert_customer_record(event_unique_id, message_json, message_dict, stub_data=False):
    logging.debug("insert_customer_record called for %s" % event_unique_id)
    data_to_insert = {}
    if stub_data:
        # We're inserting a "fake" customer record so a service_logins or devices insert can succeed.
        # We assume the real "verified" customer creation event will come later
        data_to_insert = {
            'insert_id':        event_unique_id,
            'fxa_id':           message_dict['fxa_id'],
            'fxa_ts':           message_dict['fxa_ts'],
            'email':            None,
            'service':          transform('service', message_dict),
            'create_ts':        message_dict['fxa_ts'],
            'locale':           None,
            'lang':             None,
            'marketing_opt_in': False,
            'metrics_context':  None,
        }
    else:
        data_to_insert = {
            'insert_id':         event_unique_id,
            'fxa_id':            transform('uid',              message_dict),
            'fxa_ts':            transform('ts',               message_dict),
            'email':             transform('email',            message_dict),
            'service':           transform('service',          message_dict),
            'create_ts':         transform('ts',               message_dict),
            'locale':            transform('locale',           message_dict),
            'lang':              transform('lang',             message_dict),
            'marketing_opt_in':  transform('marketing_opt_in', message_dict),
            'metrics_context':   transform('metrics_context',  message_dict),
        }
    try:
        safe_batch_insert2( event_unique_id, 'customer_record', data_to_insert )
    except KeyError as e:
        insert_failed_row(event_unique_id, message_json, 'customer_record', "Missing key: %s" % str(e))
    logging.debug("insert_customer_record finished for %s" % event_unique_id)


def insert_raw_event(event_unique_id, message_json, message_dict):
    try:
        safe_batch_insert2(
            event_unique_id,
            'raw_events',
            {
                'insert_id':      event_unique_id,
                'fxa_id':         transform('uid', message_dict),
                'fxa_ts':         transform('ts',  message_dict),
                'raw_event_json': message_json,
                'insert_ts':      spanner.COMMIT_TIMESTAMP,
            })
    except KeyError as e:
        insert_failed_row(event_unique_id, message_json, 'raw_events', "Missing key: %s" % str(e))


def pubsub_callback(message):
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
        message.ack()
        return

    logging.info(f"uid={payload_dict['uid'][:10]}... eui={event_unique_id[:10]}... ts={unixtime_to_ts(payload_dict['ts'])} -- {payload_dict['event']} event received. ")
    if int(payload_dict['ts']) % 3600 == 0:
        logging.warning(f"processing data from {unixtime_to_ts(payload_dict['ts'])}")

    if payload_dict['event'] == 'delete':
        # "delete" event - remove user from all tables, including raw_events
        handle_delete(event_unique_id, payload_json, payload_dict)

    else:
        # NOT a delete event
        # pass the actual event json to insert_raw_event
        insert_raw_event(event_unique_id, payload_json, payload_dict)

        if payload_dict['event'] == 'verified':
            insert_customer_record(event_unique_id, payload_json, payload_dict)

        elif payload_dict['event'] == 'login':
            logging.debug("processing login event")
            insert_service_login(event_unique_id, payload_json, payload_dict)

        elif payload_dict['event'] == 'device:create':
            insert_device(event_unique_id, payload_json, payload_dict)

        elif payload_dict['event'] == 'device:delete':
            delete_device(event_unique_id, payload_json, payload_dict)

        elif payload_dict['event'] == 'primaryEmailChanged':
            update_customer_email(event_unique_id, payload_json, payload_dict)

        elif (payload_dict['event'] == 'profileDataChanged' or
              payload_dict['event'] == 'passwordChange' or
              payload_dict['event'] == 'reset'):
            # uncomment when historical events are loaded
            #update_customer_last_activity(event_unique_id, payload_json, payload_dict)
            pass

        else:
            print("WARNING: Unknown event '%s'" % payload_dict['event'])

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
    for table_name in ['raw_events', 'failed_inserts', 'customer_record', 'service_logins', 'devices']:
        create_table(table_name)
    listen_loop()