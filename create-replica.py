import boto3
import botocore
import sys

aws_profile = raw_input('Enter your AWS Profile name: ')
session = boto3.Session(profile_name=aws_profile )
rds = session.client('rds')
waiter = rds.get_waiter('db_instance_available')

parent_name = raw_input('Enter the database name: ')
target_name = parent_name + '-schema-update'

def create_replica(parent_database,target_database):
    try:
        print "Attempting to create replica: Parent Instance: {} Target Instance: {}".format( parent_database, target_database)
        replica_instance = rds.create_db_instance_read_replica( DBInstanceIdentifier=target_database, SourceDBInstanceIdentifier=parent_database)
        print "Waiting for Target Instance {} to be created.".format(target_database)
        waiter.wait(DBInstanceIdentifier=target_database)
        print "Instance Created."
    except botocore.exceptions.ClientError as e:
        print e.message
        sys.exit(1)

if __name__ == '__main__':
    create_replica(parent_name, target_name)
