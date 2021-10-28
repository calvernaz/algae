from algae.rds import (
    clone_cluster,
    create_cluster_db_instances,
    upgrade_clone_cluster,
    upgrade_clone_cluster_identifier,
)
from algae.timing import timeit


@timeit
def upgrade_cluster_version(args):
    if (
        args.cluster_identifier is not None
        and args.source_cluster_identifier is not None
    ):
        clone_cluster(
            cluster_identifier=args.cluster_identifier,
            source_cluster_identifier=args.source_cluster_identifier,
            subnet_group_name=args.subnet_group_name,
        )

        if args.engine_version is not None:
            db_instance_class = "db.t3.small"
            create_cluster_db_instances(
                args.new_cluster_identifier,
                engine_version=args.engine_version,
                db_instance_class=db_instance_class,
            )

            upgrade_clone_cluster(args.cluster_identifier, args.engine_version)

            upgrade_clone_cluster_identifier(
                cluster_identifier=args.source_cluster_identifier,
            )
            upgrade_clone_cluster_identifier(
                cluster_identifier=args.cluster_identifier,
                new_cluster_identifier=args.source_cluster_identifier,
            )
