#!/usr/bin/env python3

import os, sys

from google.api_core import exceptions

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from fxa_ingest import fxa_ingest_spanner_v3

try:
    fxa_ingest_spanner_v3.create_database()
except exceptions.AlreadyExists as e:
    pass
for table_name in ['raw_events', 'failed_inserts', 'customer_record', 'service_logins', 'devices']:
    fxa_ingest_spanner_v3.create_table(table_name)
fxa_ingest_spanner_v3.listen_loop()
