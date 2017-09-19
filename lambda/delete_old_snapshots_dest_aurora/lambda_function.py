'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# aurora_delete_old_snapshots_dest
# This lambda function will delete manual snapshots that have expired in the region specified in the environment variable DEST_REGION, and according to the environment variables PATTERN and RETENTION_DAYS.
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
# Set DEST_REGION to the destination AWS region
# Set RETENTION_DAYS to the amount of days snapshots need to be kept before deleting
import boto3
import time
import os
import logging
from datetime import datetime
import re

# Initialize everything
DEST_REGION = os.getenv('DEST_REGION', os.getenv('AWS_DEFAULT_REGION')).strip()
LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('PATTERN', 'ALL_SNAPSHOTS')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS'))
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

SUPPORTED_ENGINES = [ 'aurora' ]

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


class SnapshotToolException(Exception):
    pass


# Returns a dict with only shared snapshots filtered by PATTERN, with DBSnapshotIdentifier as key and the response as attribute

def get_own_snapshots(response):
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        if snapshot['SnapshotType'] == 'manual' and re.search(PATTERN, snapshot['DBClusterSnapshotIdentifier']) and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

        elif snapshot['SnapshotType'] == 'manual' and PATTERN == 'ALL_SNAPSHOTS' and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

    return filtered


# Search for a tag indicating we copied this snapshot

def search_tag(response):
    try:
        for tag in response['TagList']:
            if tag['Key'] == 'CopiedBy' and tag['Value'] == 'Snapshot Tool for Aurora':
                return True

    except Exception:
        return False

    return False


# Return timestamp for a snapshot

def get_timestamp(snapshot_identifier, snapshot_list):
    PATTERN = '%s-(.+)' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']
    date_time = re.search(PATTERN, snapshot_identifier)
    if date_time is not None:
        try:
            return datetime.strptime(date_time.group(1), TIMESTAMP_FORMAT)
        except Exception:
            return None

    return None


def paginate_api_call(client, api_call, objecttype, *args, **kwargs):
#Takes an RDS boto client and paginates through api_call calls and returns a list of objects of objecttype
    response = {}
    kwargs_string = ','.join([ '%s=%s' % (arg,value) for arg,value in kwargs.items() ])

    if kwargs:
        temp_response = eval('client.%s(%s)' % (api_call, kwargs_string))
    else:
        temp_response = eval('client.%s()' % api_call)
    response[objecttype] = temp_response[objecttype][:]

    while 'Marker' in temp_response:
        if kwargs:
            temp_response = eval('client.%s(Marker="%s",%s)' % (api_call, temp_response['Marker'], kwargs_string))
        else:
            temp_response = eval('client.%s(Marker="%s")' % api_call, temp_response['Marker'])
        for obj in temp_response[objecttype]:
            response[objecttype].append(obj)

    return response



def lambda_handler(event, context):
    delete_pending = 0
    # Search for all snapshots
    client = boto3.client('rds', region_name=DEST_REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')
    # Filter out the ones not created automatically or with other methods
    filtered_list = get_own_snapshots(response)
    # for each snapshot
    for snapshot in filtered_list.keys():
        creation_date = get_timestamp(snapshot, filtered_list)
        if creation_date:
            snapshot_arn = filtered_list[snapshot]['Arn']
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot_arn)
            if search_tag(response_tags):
                difference = datetime.now() - creation_date
                days_difference = difference.total_seconds() / 3600 / 24
                # if we are past RETENTION_DAYS
                if days_difference > RETENTION_DAYS:
                    # delete it
                    logger.info('Deleting %s. %s days old' %
                                (snapshot, days_difference))
                    try:
                        client.delete_db_cluster_snapshot(
                            DBClusterSnapshotIdentifier=snapshot)
                    except Exception:
                        delete_pending += 1
                        logger.info('Could not delete %s' % snapshot)
                else:
                    logger.info('Not deleting %s. Only %s days old' %
                                (snapshot, days_difference))
            else:
                logger.info(
                    'Not deleting %s. Did not find correct tag' % snapshot)

    if delete_pending > 0:
        log_message = 'Snapshots pending delete: %s' % delete_pending
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
