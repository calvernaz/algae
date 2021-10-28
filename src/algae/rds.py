import datetime
import json
import logging
from enum import Enum
from json import JSONEncoder

import boto3
import polling2

_logger = logging.getLogger(__name__)

client = boto3.client("rds")


class RestoreType(Enum):
    """
    The type of restore to be performed.

    copy-on-write - The new DB cluster is restored as a clone of the source DB
    cluster.
    full-copy - The new DB cluster is restored as a full copy of the source DB
    cluster.
    """

    COPY_ON_WRITE = "copy-on-write"
    FULL_COPY = "full-copy"


class EngineType(Enum):
    AURORA = "aurora"
    AURORA_MYSQL = "aurora-mysql"
    AURORA_POSTGRESQL = "aurora-postgresql"
    MARIADB = "mariadb"
    MYSQL = "mysql"


def is_cluster_available(cluster_identifier: str) -> bool:
    response = client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
    _logger.info(response["DBClusters"][0]["Status"])
    return response["DBClusters"][0]["Status"] == "available"


def is_cluster_upgrading(cluster_identifier: str) -> bool:
    response = client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
    _logger.info(response["DBClusters"][0]["Status"])
    return response["DBClusters"][0]["Status"] == "upgrading"


def is_cluster_renaming(cluster_identifier: str) -> bool:
    try:
        response = client.describe_db_clusters(DBClusterIdentifier=cluster_identifier)
        _logger.info(response["DBClusters"][0]["Status"])
        return response["DBClusters"][0]["Status"] == "renaming"
    except client.exceptions.DBClusterNotFoundFault:
        _logger.warning("cluster not found, this is the side effect of " "renaming")
        return True


def is_instance_available(instance_identifier: str) -> bool:
    response = client.describe_db_instances(
        DBInstanceIdentifier=instance_identifier,
    )
    _logger.info(response["DBInstances"][0]["DBInstanceStatus"])
    return response["DBInstances"][0]["DBInstanceStatus"] == "available"


def is_snapshot_available(cluster_identifier: str, snapshot_identifier: str) -> bool:
    response = client.describe_db_cluster_snapshots(
        DBClusterIdentifier=cluster_identifier,
        DBClusterSnapshotIdentifier=snapshot_identifier,
        SnapshotType="manual",
    )
    _logger.info(response["DBClusterSnapshots"][0]["Status"])
    return response["DBClusterSnapshots"][0]["Status"] == "available"


def clone_cluster(
    cluster_identifier: str, source_cluster_identifier: str, subnet_group_name: str
):
    """
    Clone cluster
    :param cluster_identifier: the name identifier for the new cluster clone
    :param source_cluster_identifier: the name identifier of the source cluster
    :param subnet_group_name: the name of the VPC subnet where the clone will
    belong
    """

    _logger.info(
        f'cloning source cluster "{source_cluster_identifier}" with clone '
        f'identifier "{cluster_identifier}"'
    )

    response = client.restore_db_cluster_to_point_in_time(
        DBClusterIdentifier=cluster_identifier,
        RestoreType=RestoreType.COPY_ON_WRITE.value,
        SourceDBClusterIdentifier=source_cluster_identifier,
        UseLatestRestorableTime=True,
        DBSubnetGroupName=subnet_group_name,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to clone {source_cluster_identifier} with status code "
            f"{status_code}"
        )
        raise Exception(
            f"failed to clone {source_cluster_identifier} with status "
            f"code {status_code}"
        )

    polling2.poll(
        lambda: is_cluster_available(cluster_identifier), step=60, poll_forever=True
    )


def create_cluster_db_instances(
    cluster_identifier: str,
    engine_version: str,
    db_instance_class: str,
    engine: EngineType = EngineType.AURORA_MYSQL.value,
):
    """
    Create a database instance and associate to the cluster.

    :param cluster_identifier: the cluster identifier for the database instance`
    :param engine_version: engine version
    :param engine: aurora engine type
    :param db_instance_class: the instance class
    :return:
    """

    _logger.info(
        f"creating cluster database instance in cluster \"{cluster_identifier}\" with version \"{engine_version}\""
    )

    response = client.create_db_instance(
        DBInstanceIdentifier=f"{cluster_identifier}-instance",
        DBClusterIdentifier=cluster_identifier,
        DBInstanceClass=db_instance_class,
        Engine=engine,
        EngineVersion=engine_version,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to create db instance {cluster_identifier} with "
            f"status code {status_code}"
        )
        raise Exception(
            f"failed to modify {cluster_identifier} with status " f"code {status_code}"
        )

    polling2.poll(
        lambda: is_instance_available(f"{cluster_identifier}-instance"),
        step=60,
        poll_forever=True,
    )


def upgrade_clone_cluster(cluster_identifier: str, engine_version: str):
    _logger.info(
        f'modifying cluster clone "{cluster_identifier}" to engine '
        f"version {engine_version}"
    )

    response = client.modify_db_cluster(
        DBClusterIdentifier=cluster_identifier,
        ApplyImmediately=True,
        EngineVersion=engine_version,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to modify {cluster_identifier} with status code " f"{status_code}"
        )
        raise Exception(
            f"failed to modify {cluster_identifier} with status " f"code {status_code}"
        )

    _logger.info(
        f'modified clone cluster "{cluster_identifier}" with engine '
        f'version "{engine_version}"'
    )

    polling2.poll(
        lambda: is_cluster_upgrading(cluster_identifier), step=60, poll_forever=True
    )

    polling2.poll(
        lambda: is_cluster_available(cluster_identifier), step=60, poll_forever=True
    )


def upgrade_clone_cluster_identifier(
    cluster_identifier: str, new_cluster_identifier: str = None, suffix: str = "backup"
):
    if not suffix and not new_cluster_identifier:
        raise Exception("it requires suffix or a new identifier")

    if new_cluster_identifier is None:
        new_cluster_identifier = f"{cluster_identifier}-{suffix}"

    _logger.info(
        f'modifying cluster id "{cluster_identifier}" name to "'
        f'{new_cluster_identifier}"'
    )

    response = client.modify_db_cluster(
        DBClusterIdentifier=cluster_identifier,
        ApplyImmediately=True,
        NewDBClusterIdentifier=new_cluster_identifier,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to modify {cluster_identifier} with status code " f"{status_code}"
        )
        raise Exception(
            f"failed to modify {cluster_identifier} with status " f"code {status_code}"
        )

    polling2.poll(
        lambda: is_cluster_renaming(cluster_identifier), step=60, poll_forever=True
    )

    polling2.poll(
        lambda: is_cluster_available(new_cluster_identifier), step=60, poll_forever=True
    )


def create_db_cluster_snapshot(cluster_identifier: str, snapshot_identifier: str):
    _logger.info(
        f'creating cluster snapshot from "{cluster_identifier}" with identifier "{snapshot_identifier}"'
    )

    response = client.create_db_cluster_snapshot(
        DBClusterIdentifier=cluster_identifier,
        DBClusterSnapshotIdentifier=snapshot_identifier,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to snapshot {cluster_identifier} with status code {status_code}"
        )
        raise Exception(
            f"failed to snapshot {cluster_identifier} with status code {status_code}"
        )

    polling2.poll(
        lambda: is_snapshot_available(
            cluster_identifier=cluster_identifier,
            snapshot_identifier=snapshot_identifier,
        ),
        step=60,
        poll_forever=True,
    )


def restore_cluster_from_snapshot(
    snapshot_identifier: str, new_cluster_identifier: str, engine_type: EngineType
):
    _logger.info(
        f'restoring cluster from snapshot "{snapshot_identifier}" with identifier "{new_cluster_identifier}"'
    )
    response = client.restore_db_cluster_from_snapshot(
        DBClusterIdentifier=new_cluster_identifier,
        SnapshotIdentifier=snapshot_identifier,
        Engine=engine_type,
    )

    _logger.debug(json.dumps(response, cls=SimpleJSONEncoder))

    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code != 200:
        _logger.error(
            f"failed to restore cluster {new_cluster_identifier} with status code {status_code}"
        )
        raise Exception(
            f"failed to restore {new_cluster_identifier} with status code {status_code}"
        )

    polling2.poll(
        lambda: is_cluster_available(new_cluster_identifier), step=60, poll_forever=True
    )


class SimpleJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

        return json.JSONEncoder.default(self, o)
