'''
Copyright 2017  Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# copy_snapshots_dest_aurora
# This lambda function will copy source Aurora snapshots that match the regex specified in the environment variable PATTERN, into the account where it runs. If the snapshot is source and exists in the local region, it will copy it to the region specified in the environment variable DEST_REGION. If it finds that the snapshots are source, exist in the local and destination regions, it will delete them from the local region. Copying snapshots cross-account and cross-region need to be separate operations. This function will need to run as many times necessary for the workflow to complete.
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
# Set DEST_REGION to the destination AWS region
import boto3
from datetime import datetime
import time
import os
import logging
import re
from snapshots_tool_utils import *

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


logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())



def lambda_handler(event, context):
    # Describe all snapshots
    pending_copies = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')

    source_snapshots = get_own_snapshots_source(PATTERN, response)
    own_snapshots = get_own_snapshots_dest(PATTERN, response)

    # Get list of snapshots in DEST_REGION
    client_dest = boto3.client('rds', region_name=DESTINATION_REGION)
    response_dest = paginate_api_call(client_dest, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')
    own_dest_snapshots = get_own_snapshots_dest(PATTERN, response_dest)

    for source_identifier, source_attributes in source_snapshots.items():
        # Copy to DESTINATION_REGION
        if source_identifier not in own_dest_snapshots.keys() and source_identifier in own_snapshots.keys() and REGION != DESTINATION_REGION:
            if own_snapshots[source_identifier]['Status'] == 'available':
                try:
                    copy_remote(source_identifier, own_snapshots[source_identifier])
                 
                except Exception:
                    pending_copies += 1
                    logger.error('Remote copy pending: %s: %s' % (
                        source_identifier, own_snapshots[source_identifier]['Arn']))
            else:
                pending_copies += 1
                logger.error('Remote copy pending: %s: %s' % (
                    source_identifier, own_snapshots[source_identifier]['Arn']))


    if pending_copies > 0:
        log_message = 'Copies pending: %s. Needs retrying' % pending_copies
        logger.error(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
