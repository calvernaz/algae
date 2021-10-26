import argparse
import logging
import sys

from algae import __version__

__author__ = "Cesar Alvernaz"
__copyright__ = "Cesar Alvernaz"
__license__ = "MIT"

from algae.config import setup_logging
from algae.rds import clone_cluster, upgrade_clone_cluster, \
    create_cluster_db_instances, upgrade_clone_cluster_identifier

_logger = logging.getLogger(__name__)


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.
def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="AWS Aurora RDS blue-green upgrade tool")

    parser.add_argument(
        "--version",
        action="version",
        version="algae {ver}".format(ver=__version__),
    )
    parser.add_argument(
        "--engine-version",
        help="upgrade aurora version"
    )
    parser.add_argument(
        "--clone-identifier",
        help="name identifier of the clone"
    )
    parser.add_argument(
        "--source-cluster-identifier",
        help="name identifier of the source cluster"
    )
    parser.add_argument(
        "--subnet-group-name",
        help="name of VPC subnet group"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    if len(args) == 0:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args(args)


def main(args):
    """Wrapper allowing :func:`fib` to be called with string arguments in a
    CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)

    # db_subnet_group_name = "default-vpc-0fe4153faea0fd77f"

    # once a clone identifier is provided we will create a clone of the cluster
    if args.clone_identifier is not None and \
        args.source_cluster_identifier is not None:
        clone_cluster(
            cluster_identifier=args.clone_identifier,
            source_cluster_identifier=args.source_cluster_identifier,
            subnet_group_name=args.subnet_group_name
        )

        if args.engine_version is not None:
            create_cluster_db_instances(
                args.clone_identifier,
            )
            upgrade_clone_cluster(args.clone_identifier, args.engine_version)

            upgrade_clone_cluster_identifier(
                cluster_identifier=args.source_cluster_identifier,
            )
            upgrade_clone_cluster_identifier(
                cluster_identifier=args.clone_identifier,
                new_cluster_identifier=args.source_cluster_identifier
            )


def run():
    """Calls :func:`main` passing the CLI arguments extracted from
    :obj:`sys.argv`

    This function can be used as entry point to create console scripts with
    setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
