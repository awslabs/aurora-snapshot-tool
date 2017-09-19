'''
Copyright 2017  Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# aurora_copy_snapshots_dest
# This lambda function will copy shared Aurora snapshots that match the regex specified in the environment variable PATTERN, into the account where it runs. If the snapshot is shared and exists in the local region, it will copy it to the region specified in the environment variable DEST_REGION. If it finds that the snapshots are shared, exist in the local and destination regions, it will delete them from the local region. Copying snapshots cross-account and cross-region need to be separate operations. This function will need to run as many times necessary for the workflow to complete.
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
# Set DEST_REGION to the destination AWS region
import boto3
from datetime import datetime
import time
import os
import logging
import re

# Initialize everything
LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('SNAPSHOT_PATTERN', 'ALL_SNAPSHOTS')
DESTINATION_REGION = os.getenv('DEST_REGION').strip()
KMS_KEY_DEST_REGION = os.getenv('KMS_KEY_DEST_REGION', 'None').strip()
KMS_KEY_SOURCE_REGION = os.getenv('KMS_KEY_SOURCE_REGION', 'None').strip()
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS'))
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


def get_snapshot_identifier(snapshot):
# Function that will return the Snapshot identifier given an ARN
    match = re.match('arn:aws:rds:.*:.*:cluster-snapshot:(.+)',
                     snapshot['DBClusterSnapshotArn'])
    return match.group(1)



def get_shared_snapshots(response):
# Returns a dict with only shared snapshots filtered by PATTERN, with DBSnapshotIdentifier as key and the response as attribute
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:
        if snapshot['SnapshotType'] == 'shared' and re.search(PATTERN, get_snapshot_identifier(snapshot)) and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[get_snapshot_identifier(snapshot)] = {
                'Arn': snapshot['DBClusterSnapshotIdentifier'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
            if snapshot['StorageEncrypted'] is True:
                filtered[get_snapshot_identifier(snapshot)]['KmsKeyId'] = snapshot['KmsKeyId']

        elif snapshot['SnapshotType'] == 'shared' and PATTERN == 'ALL_SNAPSHOTS' and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[get_snapshot_identifier(snapshot)] = {
                'Arn': snapshot['DBClusterSnapshotIdentifier'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}
            if snapshot['StorageEncrypted'] is True:
                filtered[get_snapshot_identifier(snapshot)]['KmsKeyId'] = snapshot['KmsKeyId']
    return filtered



def get_own_snapshots(response):
# Returns a dict  with local snapshots, filtered by PATTERN, with DBClusterSnapshotIdentifier as key and Arn, Status as attributes
    filtered = {}
    for snapshot in response['DBClusterSnapshots']:

        if snapshot['SnapshotType'] == 'manual' and re.search(PATTERN, snapshot['DBClusterSnapshotIdentifier']) and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier']}

            if snapshot['StorageEncrypted'] is True:
                filtered[snapshot['DBClusterSnapshotIdentifier']]['KmsKeyId'] = snapshot['KmsKeyId']

        elif snapshot['SnapshotType'] == 'manual' and PATTERN == 'ALL_SNAPSHOTS' and snapshot['Engine'] in SUPPORTED_ENGINES:
            filtered[snapshot['DBClusterSnapshotIdentifier']] = {
                'Arn': snapshot['DBClusterSnapshotArn'], 'Status': snapshot['Status'], 'StorageEncrypted': snapshot['StorageEncrypted'], 'DBClusterIdentifier': snapshot['DBClusterIdentifier'] }

            if snapshot['StorageEncrypted'] is True:
                filtered[snapshot['DBClusterSnapshotIdentifier']]['KmsKeyId'] = snapshot['KmsKeyId']

    return filtered

def get_timestamp(snapshot_identifier, snapshot_list):
    PATTERN = '%s-(.+)' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']
    date_time = re.search(PATTERN, snapshot_identifier)
    if date_time is not None:
        try:
            return datetime.strptime(date_time.group(1), TIMESTAMP_FORMAT)
        except Exception:
            return None

    return None



def copy_local(snapshot_identifier, snapshot_object):
    client = boto3.client('rds', region_name=REGION)

    tags = [{
            'Key': 'CopiedBy',
            'Value': 'Snapshot Tool for Aurora'
        }]

    if snapshot_object['StorageEncrypted']:
        logger.info('Copying encrypted snapshot %s locally' % snapshot_identifier)
        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier = snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier = snapshot_identifier,
            KmsKeyId = KMS_KEY_SOURCE_REGION,
            Tags = tags)
    
    else:
        logger.info('Copying snapshot %s locally' %snapshot_identifier)
        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier = snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier = snapshot_identifier,
            Tags = tags)

    return response


def copy_remote(snapshot_identifier, snapshot_object):
    client = boto3.client('rds', region_name=DESTINATION_REGION)
    
    if snapshot_object['StorageEncrypted']:
        logger.info('Copying encrypted snapshot %s to remote region %s' % (snapshot_object['Arn'], DESTINATION_REGION))
        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier = snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier = snapshot_identifier,
            KmsKeyId = KMS_KEY_DEST_REGION,
            SourceRegion = REGION,
            CopyTags = True)

    else:
        logger.info('Copying snapshot %s to remote region %s' % (snapshot_object['Arn'], DESTINATION_REGION))
        response = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier = snapshot_object['Arn'],
            TargetDBClusterSnapshotIdentifier = snapshot_identifier,
            SourceRegion = REGION,
            CopyTags = True)

    return response


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
    # Describe all snapshots
    pending_copies = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', IncludeShared=True)

    shared_snapshots = get_shared_snapshots(response)
    own_snapshots = get_own_snapshots(response)

    # Get list of snapshots in DEST_REGION
    client_dest = boto3.client('rds', region_name=DESTINATION_REGION)
    response_dest = paginate_api_call(client_dest, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')
    own_dest_snapshots = get_own_snapshots(response_dest)

    for shared_identifier, shared_attributes in shared_snapshots.items():

        if shared_identifier not in own_snapshots.keys() and shared_identifier not in own_dest_snapshots.keys():
        # Check date
            creation_date = get_timestamp(shared_identifier, shared_snapshots)
            if creation_date:
                time_difference = datetime.now() - creation_date
                days_difference = time_difference.total_seconds() / 3600 / 24

                # Only copy if it's newer than RETENTION_DAYS
                if days_difference < RETENTION_DAYS:

                    # Copy to own account
                    try:
                        copy_local(shared_identifier, shared_attributes)

                    except Exception:
                        pending_copies += 1
                        logger.error('Local copy pending: %s' % shared_identifier)

                    else:
                        if REGION != DESTINATION_REGION:
                            pending_copies += 1
                            logger.error('Remote copy pending: %s' % shared_identifier)

                else:
                    logger.info('Not copying %s locally. Older than %s days' % (shared_identifier, RETENTION_DAYS))

            else: 
                logger.info('Not copying %s locally. No valid timestamp' % shared_identifier)


        # Copy to DESTINATION_REGION
        elif shared_identifier not in own_dest_snapshots.keys() and shared_identifier in own_snapshots.keys() and REGION != DESTINATION_REGION:
            if own_snapshots[shared_identifier]['Status'] == 'available':
                try:
                    copy_remote(shared_identifier, own_snapshots[shared_identifier])
                 
                except Exception:
                    pending_copies += 1
                    logger.error('Remote copy pending: %s: %s' % (
                        shared_identifier, own_snapshots[shared_identifier]['Arn']))
            else:
                pending_copies += 1
                logger.error('Remote copy pending: %s: %s' % (
                    shared_identifier, own_snapshots[shared_identifier]['Arn']))

        # Delete local snapshots
        elif shared_identifier in own_dest_snapshots.keys() and shared_identifier in own_snapshots.keys() and own_dest_snapshots[shared_identifier]['Status'] == 'available' and REGION != DESTINATION_REGION:

            response = client.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=shared_identifier
            )

            logger.info('Deleting local snapshot: %s' % shared_identifier)

    if pending_copies > 0:
        log_message = 'Copies pending: %s. Needs retrying' % pending_copies
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
