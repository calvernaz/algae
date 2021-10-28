from algae.rds import (
    clone_cluster,
    create_cluster_db_instances,
    upgrade_clone_cluster_identifier,
)
from algae.timing import timeit


@timeit
def clone_cluster_in_time(args):
    """
    Clones the cluster at some moment in time
    :param args:
    :return:
    """
    if (
        args.cluster_identifier is not None
        and args.new_cluster_identifier is not None
        and args.subnet_group_name is not None
        and args.engine_version
    ):
        clone_cluster(
            cluster_identifier=args.new_cluster_identifier,
            source_cluster_identifier=args.cluster_identifier,
            subnet_group_name=args.subnet_group_name,
        )

        db_instance_class = "db.t3.small"
        create_cluster_db_instances(
            args.new_cluster_identifier,
            engine_version=args.engine_version,
            db_instance_class=db_instance_class,
        )

        upgrade_clone_cluster_identifier(
            cluster_identifier=args.cluster_identifier,
        )
        upgrade_clone_cluster_identifier(
            cluster_identifier=args.new_cluster_identifier,
            new_cluster_identifier=args.cluster_identifier,
        )
