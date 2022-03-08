#
# @file dbwriter.py Writing ERS info to PostgreSQL database
# This is part of the DUNE DAQ software, copyright 2020.
#  Licensing/copyright details are in the COPYING file that you should have
#  received with this code.
#

from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer('erskafka-reporting',
                         bootstrap_servers='monkafka.cern.ch:30092',
                         group_id='group1')

# Get credentials, it expects a file with the following fields, each in a different line:

# host
# port
# user
# password
# dbname

with open('.auth') as f:
    host, port, user, password, dbname = f.read().split('\n')

try:
    con = psycopg2.connect(host=host,
                           port=port,
                           user=user,
                           password=password,
                           dbname=dbname)
except:
    print('Connection to the database failed, aborting...')
    exit()

# These are the fields in the ERS messages, see erskafka/src/KafkaStream.cpp
fields = ["partition", "issue_name", "message", "severity", "usecs_since_epoch", "time",
          "qualifiers", "params", "cwd", "file_name", "function_name", "host_name",
          "package_name", "user_name", "application_name", "user_id", "process_id",
          "thread_id", "line_number", "chain"]

cur = con.cursor()

# Uncomment to clean the database
# cur.execute('''
#             DROP TABLE public."ErrorReports";
#             ''')

# Uncomment to create the table used for the database
# cur.execute('''
#             CREATE TABLE public."ErrorReports" (
#             partition           TEXT,
#             issue_name          TEXT,
#             message             TEXT,
#             severity            TEXT,
#             usecs_since_epoch   TEXT,
#             time                BIGINT,
#             qualifiers          TEXT,
#             params              TEXT,
#             cwd                 TEXT,                 
#             file_name           TEXT,
#             function_name       TEXT,       
#             host_name           TEXT,
#             package_name        TEXT,
#             user_name           TEXT,
#             application_name    TEXT,
#             user_id             TEXT,
#             process_id          INT,
#             thread_id           INT,
#             line_number         INT,
#             chain               TEXT
#            );
#            '''
#             )

# Infinte kafka loop
for message in consumer:
    print(message)
    js = json.loads(message.value)
    ls = [str(js[key]) for key in fields]

    try:
        cur.execute(f'INSERT INTO public."ErrorReports" ({",".join(fields)}) VALUES({("%s, " * len(ls))[:-2]})', ls)
    except:
        print('Query to insert in the database failed')

    # Save the insert (or any change) to the database
    con.commit()
