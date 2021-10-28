from datetime import datetime

from algae.timing import timeit
from algae.rds import (
    create_db_cluster_snapshot,
    restore_cluster_from_snapshot,
    create_cluster_db_instances,
    EngineType,
    upgrade_clone_cluster_identifier,
)


@timeit
def restore_from_snapshot(args):
    if args.cluster_identifier is not None and args.snapshot_identifier is not None:
        snapshot_identifier = (
            f'{args.snapshot_identifier}-{datetime.now().strftime("%y-%m-%d-%H")}'
        )
        create_db_cluster_snapshot(
            cluster_identifier=args.cluster_identifier,
            snapshot_identifier=snapshot_identifier,
        )

        restore_cluster_from_snapshot(
            snapshot_identifier,
            args.new_cluster_identifier,
            EngineType.AURORA_MYSQL.value,
        )

        db_instance_class = "db.t3.small"
        create_cluster_db_instances(
            args.new_cluster_identifier,
            engine_version=args.engine_version,
            db_instance_class=db_instance_class,
        )

        # rename original cluster to "original-backup"
        upgrade_clone_cluster_identifier(
            cluster_identifier=args.cluster_identifier,
        )

        # rename "new-cluster-identifier" to "original"
        upgrade_clone_cluster_identifier(
            cluster_identifier=args.new_cluster_identifier,
            new_cluster_identifier=args.cluster_identifier,
        )
