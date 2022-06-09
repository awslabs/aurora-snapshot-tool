'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# copy_snapshots_source_aurora
#
# This Lambda function copies snapshots created by aurora_take_snapshot and re-encrypts them with the KMS key
# set in the environment variable KMS_KEY_SOURCE_REGION.
#
# It will only copy snapshots tagged with both 'reEncrypt':'YES' and 'CreatedBy':'Snapshot Tool for Aurora'
import boto3
from datetime import datetime
import os
import logging
from snapshots_tool_utils import *


# Initialize from environment variable
KMS_KEY_SOURCE_REGION = os.getenv('KMS_KEY_SOURCE_REGION', 'None').strip()
LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('PATTERN', 'ALL_CLUSTERS')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS'))

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')


logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())



def lambda_handler(event, context):
    pending_copies = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', SnapshotType='manual')
    filtered = get_own_snapshots_source(PATTERN, response)

    # Fetch all manual snapshots
    for snapshot_identifier, snapshot_object in filtered.items():
        snapshot_arn = snapshot_object['Arn']
        response_tags = client.list_tags_for_resource(
            ResourceName=snapshot_arn)
    pending_copies = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', SnapshotType='manual')
    filtered = get_own_snapshots_source(PATTERN, response)

    # Find relevant snapshots
    for snapshot_identifier, snapshot_object in filtered.items():
        snapshot_arn = snapshot_object['Arn']
        response_tags = client.list_tags_for_resource(
            ResourceName=snapshot_arn)
        if snapshot_object['Status'].lower() == 'available' and search_tag_reencrypt(response_tags):
            # Check date
            creation_date = get_timestamp(snapshot_identifier, filtered)
            if creation_date:
                time_difference = datetime.now() - creation_date
                days_difference = time_difference.total_seconds() / 3600 / 24

                # Only copy if it's newer than RETENTION_DAYS
                if days_difference < RETENTION_DAYS:

                    # Copy to own account
                    try:
                        copy_local(snapshot_identifier, snapshot_object, f'{snapshot_identifier}-reencrypted')

                    except Exception as e:
                        pending_copies += 1
                        logger.error(e)
                        logger.error('Local copy pending: %s' % snapshot_identifier)
                    else:
                        client.add_tags_to_resource(
                            ResourceName=snapshot_arn,
                            Tags=[
                                {
                                    'Key': 'reEncrypt',
                                    'Value': 'NO'
                                },
                            ]
                        )
                else:
                    logger.info('Not copying %s locally. Older than %s days' % (snapshot_identifier, RETENTION_DAYS))

            else:
                logger.info('Not copying %s locally. No valid timestamp' % snapshot_identifier)

    if pending_copies > 0:
        log_message = 'Copies pending: %s. Needs retrying' % pending_copies
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
