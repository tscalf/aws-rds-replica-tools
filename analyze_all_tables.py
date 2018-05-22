#!/usr/bin/env python
'''
Analyze all non-system tables.
'''

import json
import logging
import os
import boto3
import pymysql

LOGGER = logging.getLogger('table_analysis')
LOGGER.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
FH = logging.FileHandler('table_analysis.log')
FH.setLevel(logging.DEBUG)
# create console handler with a higher log level
CH = logging.StreamHandler()
CH.setLevel(logging.INFO)
# create formatter and add it to the handlers
FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
FH.setFormatter(FORMATTER)
CH.setFormatter(FORMATTER)
# add the handlers to the logger
LOGGER.addHandler(FH)
LOGGER.addHandler(CH)


def get_db_credentials(landscape, environment, dbinstanceidentifier, dbschema):
    '''
    Get the credentials for the database from SSM.
    :param landscape: The landscape where the product is deployed.
        Typical Contry Code: uk, eu, na, us, ca
    :param environment: The environment for the product. Typically: preprod or production
    :param dbinstanceidentifier: The RDS DB instance Identifier.
    Load parameters into SSM as JSON arrays with the following command:
    aws ssm put-parameter --type SecureString \
        --name "lsm.<landscape>.<environment>.<dbinstanceidentifier>.<dbschema>" \
        --value '{"mysql_username": "<username>", "mysql_password": "<password>",\
        "EndpointAddress": "<rds instance endpoint>", "EndpointPort": 3306 }' \
        --region us-east-2 --overwrite
    '''
    session = boto3.Session(region_name='us-east-2')
    client = session.client('ssm')

    separator = '.'
    ssm_key = separator.join(["lsm", landscape, environment, dbinstanceidentifier, dbschema])
    ssm_vars = client.get_parameters(Names=[ssm_key], WithDecryption=True)
    # LOGGER.debug("SSM Paramters: %s", ssm_vars)
    return json.loads(ssm_vars['Parameters'][0]['Value'])

def main():
    '''
    Do some work
    '''
    if 'LANDSCAPE' not in os.environ:
        os.environ['LANDSCAPE'] = raw_input("Enter the landscape of the taget instance: ")
    landscape = os.environ['LANDSCAPE']
    if 'ENVIRONMENT' not in os.environ:
        os.environ['ENVIRONMENT'] = raw_input("Enter the environment of the taget instance: ")
    environment = os.environ['ENVIRONMENT']
    if 'HOST_ID' not in os.environ:
        os.environ['HOST_ID'] = raw_input("Enter the host id of the taget instance: ")
    host_id = os.environ['HOST_ID']
    if 'DBSCHEMA' not in os.environ:
        os.environ['DBSCHEMA'] = raw_input("Enter the schema name to convert: ")
    dbschema = os.environ['DBSCHEMA']
    creds = get_db_credentials(landscape=landscape, environment=environment,
                               dbinstanceidentifier=host_id, dbschema=dbschema)
    if 'HOST_OVERIDE' in os.environ:
        if 'DB_HOST' not in os.environ:
            os.environ['DB_HOST'] = raw_input("Enter the Override Hostname")
        db_host = os.environ['DB_HOST']
    else:
        db_host = creds['EndpointAddress']

    LOGGER.debug("DEBUG: Connecting to database %s", db_host)
    try:
        mysql_connect = pymysql.connect(
            host=db_host.encode('ascii', 'ignore'),
            port=3306, database=dbschema,
            user=creds['mysql_username'], password=creds['mysql_password'],
            connect_timeout=5)
        LOGGER.debug("Connected as: %s", creds['mysql_username'])
    except Exception as exc:
        LOGGER.error("Failed to connect to %s", db_host)
        LOGGER.error("Error: %s", exc.args)
        return

    try:
        cursor = mysql_connect.cursor()
        query = "SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables \
            WHERE table_schema = '{}'".format(dbschema)
        LOGGER.debug("DEBUG: Executing query: %s", query)
        cursor.execute(query)
        for row in cursor.fetchall():
            LOGGER.info("Analyzing table %s ", row[0])
            LOGGER.debug("executing query: ANALYZE TABLE  %s", row[0])
            cursor.execute("ANALYZE TABLE {}".format(row[0]))
            LOGGER.debug("Table analysis complete for table: %s.", row[0])
        cursor.close()
        mysql_connect.commit()
    except Exception as exc:
        LOGGER.error("Query execution failed. %s", exc)
    finally:
        mysql_connect.close()
    LOGGER.info("Table analysis complete for schema: %s", dbschema)

if __name__ == '__main__':
    main()
