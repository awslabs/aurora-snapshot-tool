'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''


# take_snapshots_aurora
# This lambda function takes a snapshot of Aurora clusters according to the environment variable PATTERN and INTERVAL
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
# Set INTERVAL to the amount of hours between backups. This function will list available manual snapshots and only trigger a new one if the latest is older than INTERVAL hours
import boto3
from datetime import datetime
import time
import os
import logging
import re

# Initialize everything
LOGLEVEL = os.getenv('LOG_LEVEL').strip()
BACKUP_INTERVAL = int(os.getenv('INTERVAL', '24'))
PATTERN = os.getenv('PATTERN', 'ALL_CLUSTERS')

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')

SUPPORTED_ENGINES = [ 'aurora' ] 

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


class SnapshotToolException(Exception):
    pass


def search_tag(response):
# Takes a describe_db_cluster_snapshots response and searches for our shareAndCopy tag
    try:

        for tag in response['TagList']:
            if tag['Key'] == 'CreatedBy' and tag['Value'] == 'Snapshot Tool for Aurora': return True

    except Exception: return False

    else: return False


def filter_clusters(PATTERN, cluster_list):
# Takes the response from describe-db-clusters and filters according to PATTERN in DBClusterIdentifier
    filtered_list = []

    for cluster in cluster_list['DBClusters']:

        if PATTERN == 'ALL_CLUSTERS' and cluster['Engine'] in SUPPORTED_ENGINES:
            filtered_list.append(cluster)

        else:
            match = re.search(PATTERN, cluster['DBClusterIdentifier'])

            if match and cluster['Engine'] in SUPPORTED_ENGINES:
                filtered_list.append(cluster)

    return filtered_list



def get_own_snapshots(response):
# Filters our own snapshots
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        client = boto3.client('rds', region_name=REGION)
        response_tags = client.list_tags_for_resource(
            ResourceName=snapshot['DBClusterSnapshotArn'])

        if snapshot['SnapshotType'] == 'manual' and re.search(PATTERN, snapshot['DBClusterSnapshotIdentifier']) and snapshot['Engine'] in SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])

            if search_tag(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

        elif snapshot['SnapshotType'] == 'manual' and PATTERN == 'ALL_CLUSTERS' and snapshot['Engine'] in SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])
            
            if search_tag(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

    return filtered



def get_timestamp_no_minute(snapshot_identifier, snapshot_list):
# Get a timestamp from the name of a snapshot and strip out the minutes
    PATTERN = '%s-(.+)-\d{2}' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']
    date_time = re.search(PATTERN, snapshot_identifier)
    if date_time is not None:
        return datetime.strptime(date_time.group(1), '%Y-%m-%d-%H')



def get_latest_snapshot_ts(cluster_identifier, filtered_snapshots):
# Get latest snapshot for a specific DBClusterIdentifier
    client = boto3.client('rds', REGION)
    timestamps = []
    for snapshot,snapshot_object in filtered_snapshots.items():
        if snapshot_object['DBClusterIdentifier'] == cluster_identifier:
            timestamp = get_timestamp_no_minute(snapshot, filtered_snapshots)
            if timestamp is not None:
                timestamps.append(timestamp)
    if len(timestamps) > 0:
        return max(timestamps)
    else:
        return None



def requires_backup(cluster, filtered_snapshots):
# Returns True if latest snapshot is older than INTERVAL
    latest = get_latest_snapshot_ts(cluster['DBClusterIdentifier'], filtered_snapshots)
    if latest is not None:
        backup_age = datetime.now() - latest
        if backup_age.total_seconds() >= (BACKUP_INTERVAL * 60 * 60):
            return True
        else:
            return False
    elif latest is None:
        return True


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

    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_clusters', 'DBClusters')
    now = datetime.now()
    pending_backups = 0
    filtered_clusters = filter_clusters(PATTERN, response)
    filtered_snapshots = get_own_snapshots(paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots'))

    for db_cluster in filtered_clusters:

        timestamp_format = now.strftime('%Y-%m-%d-%H-%M')

        if requires_backup(db_cluster, filtered_snapshots):

            backup_age = get_latest_snapshot_ts(
                db_cluster['DBClusterIdentifier'],
                filtered_snapshots)

            if backup_age is not None:
                logger.info('Backing up %s. Backed up %s minutes ago' % (
                    db_cluster['DBClusterIdentifier'], ((now - backup_age).total_seconds() / 60)))

            else:
                logger.info('Backing up %s. No previous backup found' %
                            db_cluster['DBClusterIdentifier'])

            snapshot_identifier = '%s-%s' % (
                db_cluster['DBClusterIdentifier'], timestamp_format)

            try:
                response = client.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier=snapshot_identifier,
                    DBClusterIdentifier=db_cluster['DBClusterIdentifier'],
                    Tags=[{'Key': 'CreatedBy', 'Value': 'Snapshot Tool for Aurora'}, {
                        'Key': 'CreatedOn', 'Value': timestamp_format}, {'Key': 'shareAndCopy', 'Value': 'YES'}]
                )
            except Exception:
                pending_backups += 1
        else:

            backup_age = get_latest_snapshot_ts(
                db_cluster['DBClusterIdentifier'],
                filtered_snapshots)

            logger.info('Skipped %s. Does not require backup. Backed up %s minutes ago' % (
                db_cluster['DBClusterIdentifier'], (now - backup_age).total_seconds() / 60))

    if pending_backups > 0:
        log_message = 'Could not back up every cluster. Backups pending: %s' % pending_backups
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
