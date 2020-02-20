'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''


# snapshots_tool_utils
# Support module for the Snapshots Tool for Aurora

import boto3
from datetime import datetime, timezone
import time
import os
import logging
import re


# Initialize everything
_LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()

_DEST_ACCOUNTID = str(os.getenv('DEST_ACCOUNT', '000000000000')).strip()

_DESTINATION_REGION = os.getenv(
    'DEST_REGION', os.getenv('AWS_DEFAULT_REGION')).strip()

_KMS_KEY_DEST_REGION = os.getenv('KMS_KEY_DEST_REGION', 'None').strip()

_KMS_KEY_SOURCE_REGION = os.getenv('KMS_KEY_SOURCE_REGION', 'None').strip()

_TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    _REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    _REGION = os.getenv('AWS_DEFAULT_REGION')

_SUPPORTED_ENGINES = [ 'aurora', 'aurora-mysql', 'aurora-postgresql', 'neptune']

_AUTOMATED_BACKUP_LIST = []

logger = logging.getLogger()
logger.setLevel(_LOGLEVEL.upper())


class SnapshotToolException(Exception):
    pass


def search_tag_created(response):
    # Takes a describe_db_cluster_snapshots response and searches for our shareAndCopy tag
    try:

        for tag in response['TagList']:
            if tag['Key'] == 'CreatedBy' and tag['Value'] == 'Snapshot Tool for Aurora':
                return True

    except Exception:
        return False

    else:
        return False


def filter_clusters(pattern, cluster_list):
    # Takes the response from describe-db-clusters and filters according to pattern in DBClusterIdentifier
    filtered_list = []

    for cluster in cluster_list['DBClusters']:

        if pattern == 'ALL_CLUSTERS' and cluster['Engine'] in _SUPPORTED_ENGINES:
            filtered_list.append(cluster)

        else:
            match = re.search(pattern, cluster['DBClusterIdentifier'])

            if match and cluster['Engine'] in _SUPPORTED_ENGINES:
                filtered_list.append(cluster)

    return filtered_list


def get_snapshot_identifier(snapshot):
    # Function that will return the Snapshot identifier given an ARN
    match = re.match('arn:aws:rds:.*:.*:cluster-snapshot:(.+)',
                     snapshot['DBClusterSnapshotArn'])
    return match.group(1)


def get_own_snapshots_source(pattern, response):
    # Filters our own snapshots
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        if snapshot['SnapshotType'] == 'manual' and re.search(pattern, snapshot['DBClusterIdentifier']) and snapshot['Engine'] in _SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=_REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])

            if search_tag_created(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
        #Changed the next line to search for ALL_CLUSTERS or ALL_SNAPSHOTS so it will work with no-x-account
        elif snapshot['SnapshotType'] == 'manual' and (pattern == 'ALL_CLUSTERS' or pattern == 'ALL_SNAPSHOTS') and snapshot['Engine'] in _SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=_REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])

            if search_tag_created(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

    return filtered

def get_own_snapshots_no_x_account(pattern, response, REGION):
    # Filters our own snapshots
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        if snapshot['SnapshotType'] == 'manual' and re.search(pattern, snapshot['DBClusterIdentifier']) and snapshot['Engine'] in _SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])

            if search_tag_created(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
        #Changed the next line to search for ALL_CLUSTERS or ALL_SNAPSHOTS so it will work with no-x-account
        elif snapshot['SnapshotType'] == 'manual' and pattern == 'ALL_SNAPSHOTS' and snapshot['Engine'] in _SUPPORTED_ENGINES:
            client = boto3.client('rds', region_name=REGION)
            response_tags = client.list_tags_for_resource(
                ResourceName=snapshot['DBClusterSnapshotArn'])

            if search_tag_created(response_tags):
                filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                    'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

    return filtered

def get_own_snapshots_share(pattern, response):
    # Filter manual snapshots by pattern. Returns a dict of snapshots with DBClusterSnapshotIdentifier as key and Status, DBClusterIdentifier as attributes
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:
        if snapshot['SnapshotType'] == 'manual' and re.search(pattern, snapshot['DBClusterIdentifier']) and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
        elif snapshot['SnapshotType'] == 'manual' and pattern == 'ALL_CLUSTERS' and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
    return filtered


def get_shared_snapshots(pattern, response):
    # Returns a dict with only shared snapshots filtered by pattern, with DBSnapshotIdentifier as key and the response as attribute
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:
        if snapshot['SnapshotType'] == 'shared' and re.search(pattern, snapshot['DBClusterIdentifier']) and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[get_snapshot_identifier(snapshot)] = {
                'Arn': snapshot['DBClusterSnapshotIdentifier'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
            if snapshot['StorageEncrypted'] is True:
                filtered[get_snapshot_identifier(
                    snapshot)]['KmsKeyId'] = snapshot['KmsKeyId']

        elif snapshot['SnapshotType'] == 'shared' and pattern == 'ALL_SNAPSHOTS' and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[get_snapshot_identifier(snapshot)] = {
                'Arn': snapshot['DBClusterSnapshotIdentifier'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
            if snapshot['StorageEncrypted'] is True:
                filtered[get_snapshot_identifier(
                    snapshot)]['KmsKeyId'] = snapshot['KmsKeyId']
    return filtered


def get_own_snapshots_dest(pattern, response):
    # Returns a dict  with local snapshots, filtered by pattern, with DBClusterSnapshotIdentifier as key and Arn, Status as attributes
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        if snapshot['SnapshotType'] == 'manual' and re.search(pattern, snapshot['DBClusterIdentifier']) and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

            if snapshot['StorageEncrypted'] is True:
                filtered[snapshot['DBClusterSnapshotIdentifier']
                         ]['KmsKeyId'] = snapshot['KmsKeyId']

        elif snapshot['SnapshotType'] == 'manual' and pattern == 'ALL_SNAPSHOTS' and snapshot['Engine'] in _SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

            if snapshot['StorageEncrypted'] is True:
                filtered[snapshot['DBClusterSnapshotIdentifier']
                         ]['KmsKeyId'] = snapshot['KmsKeyId']

    return filtered


def copy_local(snapshot_identifier, snapshot_object):
    client = boto3.client('rds', region_name=_REGION)

    tags = [{
            'Key': 'CopiedBy',
            'Value': 'Snapshot Tool for Aurora'
            }]

    if snapshot_object['StorageEncrypted']:
        logger.info('Copying encrypted snapshot %s locally' %
                    snapshot_identifier)

        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier=snapshot_identifier,
            KmsKeyId=_KMS_KEY_SOURCE_REGION,
            Tags=tags)

    else:
        logger.info('Copying snapshot %s locally' % snapshot_identifier)

        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier=snapshot_identifier,
            Tags=tags)

    return response


def copy_remote(snapshot_identifier, snapshot_object):
    client = boto3.client('rds', region_name=_DESTINATION_REGION)

    if snapshot_object['StorageEncrypted']:
        logger.info('Copying encrypted snapshot %s to remote region %s' %
                    (snapshot_object['Arn'], _DESTINATION_REGION))

        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier=snapshot_identifier,
            KmsKeyId=_KMS_KEY_DEST_REGION,
            SourceRegion=_REGION,
            CopyTags=True)

    else:
        logger.info('Copying snapshot %s to remote region %s' %
                    (snapshot_object['Arn'], _DESTINATION_REGION))

        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier=snapshot_identifier,
            SourceRegion=_REGION,
            CopyTags=True)

    return response


def get_timestamp(snapshot_identifier, snapshot_list):

    # Searches for a timestamp on a snapshot name
    pattern = '%s-(.+)' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']

    date_time = re.search(pattern, snapshot_identifier)

    if date_time is not None:
        try:
            return datetime.strptime(date_time.group(1), _TIMESTAMP_FORMAT)

        except Exception:
            return None

    return None


def get_timestamp_no_minute(snapshot_identifier, snapshot_list):

    # Get a timestamp from the name of a snapshot and strip out the minutes
    pattern = '%s-(.+)-\d{2}' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']

    date_time = re.search(pattern, snapshot_identifier)

    if date_time is not None:
        return datetime.strptime(date_time.group(1), '%Y-%m-%d-%H')


def get_latest_snapshot_ts(cluster_identifier, filtered_snapshots):

    # Get latest snapshot for a specific DBClusterIdentifier
    timestamps = []

    for snapshot, snapshot_object in filtered_snapshots.items():

        if snapshot_object['DBClusterIdentifier'] == cluster_identifier:

            timestamp = get_timestamp_no_minute(snapshot, filtered_snapshots)

            if timestamp is not None:

                timestamps.append(timestamp)

    if len(timestamps) > 0:

        return max(timestamps)

    else:
        return None


def requires_backup(backup_interval, cluster, filtered_snapshots):

    # Returns True if latest snapshot is older than INTERVAL
    latest = get_latest_snapshot_ts(
        cluster['DBClusterIdentifier'], filtered_snapshots)

    if latest is not None:

        backup_age = datetime.now() - latest

        if backup_age.total_seconds() >= (backup_interval * 60 * 60):
            return True

        else:
            return False

    elif latest is None:
        return True


def paginate_api_call(client, api_call, objecttype, *args, **kwargs):
#Takes an RDS boto client and paginates through api_call calls and returns a list of objects of objecttype
    response = {}
    response[objecttype] = []

    # Create a paginator
    paginator = client.get_paginator(api_call)

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(**kwargs)
    for page in page_iterator:
        for item in page[objecttype]:
            response[objecttype].append(item)

    return response


def search_tag_share(response):
    # Takes a describe_db_cluster_snapshots response and searches for our shareAndCopy tag
    try:

        for tag in response['TagList']:

            if tag['Key'] == 'shareAndCopy' and tag['Value'] == 'YES':

                for tag2 in response['TagList']:

                    if tag2['Key'] == 'CreatedBy' and tag2['Value'] == 'Snapshot Tool for Aurora':

                        return True

    except Exception:
        return False

    return False


def search_tag_copied(response):
    try:

        for tag in response['TagList']:

            if tag['Key'] == 'CopiedBy' and tag['Value'] == 'Snapshot Tool for Aurora':
                return True

    except Exception:
        return False

    return False


def get_all_automated_snapshots(client):
    global _AUTOMATED_BACKUP_LIST
    if len(_AUTOMATED_BACKUP_LIST) == 0:
        response = paginate_api_call(
            client,
            'describe_db_cluster_snapshots',
            'DBClusterSnapshots',
            SnapshotType='automated',
        )
        _AUTOMATED_BACKUP_LIST = response['DBClusterSnapshots']

    return _AUTOMATED_BACKUP_LIST


def copy_or_create_db_snapshot(
    client,
    db_cluster,
    snapshot_identifier,
    snapshot_tags,
    use_automated_backup=True,
    backup_interval=24,
):

    if use_automated_backup is False:
        logger.info(
            'creating snapshot out of a running db cluster: %s'
            % db_cluster['DBClusterIdentifier']
        )
        snapshot_tags.append(
            {'Key': 'DBClusterIdentifier', 'Value': db_cluster['DBClusterIdentifier']}
        )
        return client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snapshot_identifier,
            DBClusterIdentifier=db_cluster['DBClusterIdentifier'],
            Tags=snapshot_tags,
        )

    # Find the latest automted backup and Copy snapshot out of it
    all_automated_snapshots = get_all_automated_snapshots(client)
    dbcluster_automated_snapshots = [x for x in all_automated_snapshots
                              if x['DBClusterIdentifier'] == db_cluster['DBClusterIdentifier']]

    # Raise exception if no automated backup found
    if len(dbcluster_automated_snapshots) <= 0:
        log_message = (
            'No automated snapshots found for db: %s'
            % db_cluster['DBClusterIdentifier']
        )
        logger.error(log_message)
        raise SnapshotToolException(log_message)

    # filter last automated backup
    dbcluster_automated_snapshots.sort(key=lambda x: x['SnapshotCreateTime'])
    latest_snapshot = dbcluster_automated_snapshots[-1]

    # Make sure automated backup is not more than backup_interval window old
    backup_age = datetime.now(timezone.utc) - latest_snapshot['SnapshotCreateTime']
    if backup_age.total_seconds() >= (backup_interval * 60 * 60):
        now = datetime.now()
        log_message = (
            'Last automated backup was %s minutes ago. No latest automated backup available. '
            % ((now - backup_age).total_seconds() / 60)
        )
        logger.warn(log_message)

        # If last automated backup is over 2*backup_interval, then raise error
        if backup_age.total_seconds() >= (backup_interval * 2 * 60 * 60):
            logger.error(log_message)
            raise SnapshotToolException(log_message)

    logger.info(
        'Creating snapshot out of an automated backup: %s'
        % latest_snapshot['DBClusterSnapshotIdentifier']
    )
    snapshot_tags.append(
        {
            'Key': 'SourceDBClusterSnapshotIdentifier',
            'Value': latest_snapshot['DBClusterSnapshotIdentifier'],
        }
    )
    return client.copy_db_cluster_snapshot(
        SourceDBClusterSnapshotIdentifier=latest_snapshot[
            'DBClusterSnapshotIdentifier'
        ],
        TargetDBClusterSnapshotIdentifier=snapshot_identifier,
        Tags=snapshot_tags,
        CopyTags=False,
    )
