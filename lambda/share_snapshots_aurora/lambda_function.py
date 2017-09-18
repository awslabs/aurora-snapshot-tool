'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# share_snapshots_aurora
# This Lambda function shares snapshots created by aurora_take_snapshot with the account set in the environment variable DEST_ACCOUNT
# It will only share snapshots tagged with shareAndCopy and a value of YES
import boto3
from datetime import datetime
import time
import os
import logging
import re


# Initialize from environment variable
LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
DEST_ACCOUNTID = str(os.getenv('DEST_ACCOUNT')).strip()
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
            if tag['Key'] == 'shareAndCopy' and tag['Value'] == 'YES':
                for tag2 in response['TagList']: 
                    if tag2['Key'] == 'CreatedBy' and tag2['Value'] == 'Snapshot Tool for Aurora':
                        return True

    except Exception:
        return False

    return False



def check_snapshot_shared(response):
# Returns True if a snapshot has not been shared with DEST_ACCOUNT
    try:
        for attribute in response['DBClusterSnapshotAttributesResult']['DBClusterSnapshotAttributes']:
            if attribute['AttributeName'] == 'restore':
                for value in attribute['AttributeValues']:
                    if str(value) == DEST_ACCOUNTID:
                        return True

    except Exception:
        return False

    return False


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


def lambda_handler(event, context):
    pending_snapshots = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')
    filtered = get_own_snapshots(response)

    # Search all snapshots for the correct tag
    for snapshot_identifier,snapshot_object in filtered.items():
        snapshot_arn = snapshot_object['Arn']
        response_tags = client.list_tags_for_resource(
            ResourceName=snapshot_arn)

        if snapshot_object['Status'].lower() == 'available' and search_tag(response_tags):
            try:
                # Share snapshot with dest_account
                response_modify = client.modify_db_cluster_snapshot_attribute(
                    DBClusterSnapshotIdentifier=snapshot_identifier,
                    AttributeName='restore',
                    ValuesToAdd=[
                        DEST_ACCOUNTID
                    ]
                )
            except Exception:
                logger.error('Exception sharing %s' % snapshot_identifier)
                pending_snapshots += 1

    if pending_snapshots > 0:
        log_message = 'Could not share all snapshots. Pending: %s' % pending_snapshots
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
