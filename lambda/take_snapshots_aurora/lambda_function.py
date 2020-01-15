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
import os
import logging
from snapshots_tool_utils import *

# Initialize everything
LOGLEVEL = os.getenv('LOG_LEVEL').strip()
BACKUP_INTERVAL = int(os.getenv('INTERVAL', '24'))
PATTERN = os.getenv('PATTERN', 'ALL_CLUSTERS')
ADD_NAME = os.getenv('ADD_NAME', 'NONE')

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')


logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())



def lambda_handler(event, context):

    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_clusters', 'DBClusters')
    now = datetime.now()
    pending_backups = 0
    filtered_clusters = filter_clusters(PATTERN, response)
    filtered_snapshots = get_own_snapshots_source(PATTERN, paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots'))

    for db_cluster in filtered_clusters:

        timestamp_format = now.strftime('%Y-%m-%d-%H-%M')

        if requires_backup(BACKUP_INTERVAL, db_cluster, filtered_snapshots):

            backup_age = get_latest_snapshot_ts(
                db_cluster['DBClusterIdentifier'],
                filtered_snapshots)

            if backup_age is not None:
                logger.info('Backing up %s. Backed up %s minutes ago' % (
                    db_cluster['DBClusterIdentifier'], ((now - backup_age).total_seconds() / 60)))

            else:
                logger.info('Backing up %s. No previous backup found' %
                            db_cluster['DBClusterIdentifier'])

            if ADD_NAME != 'NONE':
                snapshot_identifier = '%s-%s-%s' % (
                    ADD_NAME, db_cluster['DBClusterIdentifier'], timestamp_format
                )
            else:
                snapshot_identifier = '%s-%s' % (
                    db_cluster['DBClusterIdentifier'], timestamp_format)

            try:
                response = client.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier=snapshot_identifier,
                    DBClusterIdentifier=db_cluster['DBClusterIdentifier'],
                    Tags=[{'Key': 'CreatedBy', 'Value': 'Snapshot Tool for Aurora'}, {
                        'Key': 'CreatedOn', 'Value': timestamp_format}, {'Key': 'shareAndCopy', 'Value': 'YES'}]
                )
            except Exception as e:
                logger.error(e)
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


