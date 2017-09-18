'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# aurora_delete_old_snapshots
# This Lambda function will delete snapshots that have expired and match the regex set in the PATTERN environment variable. It will also look for a matching timestamp in the following format: YYYY-MM-DD-HH-mm
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
import boto3
from datetime import datetime
import time
import os
import logging
import re

LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('PATTERN', 'ALL_CLUSTERS')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS', '7'))
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')

SUPPORTED_ENGINES = [ 'aurora' ]

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


class SnapshotToolException(Exception):
    pass


def get_own_snapshots(response):
# Filter manual snapshots by PATTERN. Returns a dict of snapshots with DBClusterSnapshotIdentifier as key and Status, DBClusterIdentifier as attributes
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:
        if snapshot['SnapshotType'] == 'manual' and re.search(PATTERN, snapshot['DBClusterSnapshotIdentifier']) and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
        elif snapshot['SnapshotType'] == 'manual' and PATTERN == 'ALL_CLUSTERS' and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
    return filtered



def search_tag(response):
# Search for a tag indicating we created this snapshot
    try:
        for tag in response['TagList']:
            if tag['Key'] == 'CreatedBy' and tag['Value'] == 'Snapshot Tool for Aurora':
                return True

    except Exception:
        return False

    return False


def get_timestamp(snapshot_identifier, snapshot_list):
# Searches for a timestamp on a snapshot name
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
    pending_delete = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')

    filtered_list = get_own_snapshots(response)

    for snapshot in filtered_list.keys():
        creation_date = get_timestamp(snapshot, filtered_list)
        if creation_date:
            snapshot_arn = filtered_list[snapshot]['Arn']
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot_arn)
            if search_tag(response_tags):
                difference = datetime.now() - creation_date
                days_difference = difference.total_seconds() / 3600 / 24
                logger.debug('%s created %s days ago' %
                             (snapshot, days_difference))
                # if we are past RETENTION_DAYS
                if days_difference > RETENTION_DAYS:
                    # delete it
                    logger.info('Deleting %s' % snapshot)
                    try:
                        client.delete_db_cluster_snapshot(
                            DBClusterSnapshotIdentifier=snapshot)
                    except Exception:
                        pending_delete += 1
                        logger.info('Could not delete %s ' % snapshot)

    if pending_delete > 0:
        message = 'Snapshots pending delete: %s' % pending_delete
        logger.error(message)
        raise SnapshotToolException(message)


if __name__ == '__main__':
    lambda_handler(None, None)
