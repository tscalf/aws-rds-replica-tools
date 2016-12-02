
import boto3
import botocore
import sys
import time

print "This will promote a database to a stand-alone instance. This cannot be reverted."
aws_profile = raw_input('Enter your AWS Profile name: ')
session = boto3.Session(profile_name=aws_profile )
rds = session.client('rds')
waiter = rds.get_waiter('db_instance_available')

parent_name = raw_input('Enter the database name: ')
target_name = parent_name + '-schema-update'
archive_name = parent_name + '-old'

def promote_instance(parent_database):
    try:
        print "Attempting to promote replica: {}".format( parent_database)
        replica_instance = rds.promote_read_replica( DBInstanceIdentifier=parent_database)
        print "Waiting for promotion of instance: {}".format(parent_database)
        waiter.wait(DBInstanceIdentifier=parent_database)
        print "Instance {} promoted.".format(parent_database)
    except botocore.exceptions.ClientError as e:
        print e.message
        sys.exit(1)

def rename_instance(orig_name, new_name):
    try:
        print "Attempting to rename database: {} to {}".format(orig_name, new_name)
        instance = rds.modify_db_instance(DBInstanceIdentifier=orig_name, NewDBInstanceIdentifier=new_name, ApplyImmediately=True)
        # time.sleep (30)
        # print "Waiting for rename of instance: {}".format(orig_name)
        # waiter.wait(DBInstanceIdentifier=new_name)
        # print "Instance {} renamed to {}.".format(orig_name,new_name)
    except botocore.exceptions.ClientError as e:
        print e.message
        sys.exit(1)

if __name__ == '__main__':
    promote_instance(target_name)
    #TODO: Enable Backups
    #TODO: Change the parameter group so Replicas are not writable.
    rename_instance(parent_name, archive_name)
    print 'Monitor AWS console for the instance rename to complete.'
    raw_input('Press enter to continue: ')
    rename_instance(target_name, parent_name)
    print 'Continue monitoring RDS console for the instance reboots to complete.'
