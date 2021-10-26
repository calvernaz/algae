import argparse
import logging
import sys

from algae import __version__

__author__ = "Cesar Alvernaz"
__copyright__ = "Cesar Alvernaz"
__license__ = "MIT"

from algae.config import setup_logging
from algae.upgrade import upgrade_cluster_version

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
        description="AWS Aurora RDS blue-green upgrade tool"
    )

    parser.add_argument(
        "--version",
        action="version",
        version="algae {ver}".format(ver=__version__),
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

    sub_parsers = parser.add_subparsers(dest="command")

    # upgrade cluster version sub-parser
    upgrade_parser = sub_parsers.add_parser(
        "upgrade-cluster-version",
        help="upgrade cluster version",
    )
    upgrade_parser.add_argument("--engine-version", help="upgrade aurora version")
    upgrade_parser.add_argument(
        "--cluster-identifier", help="name identifier of the cluster clone"
    )
    upgrade_parser.add_argument(
        "--source-cluster-identifier", help="name identifier of the source cluster"
    )
    upgrade_parser.add_argument("--subnet-group-name", help="name of VPC subnet group")

    # delete cluster sub-parser
    delete_parser = sub_parsers.add_parser("delete-cluster", help="delete cluster")
    delete_parser.add_argument(
        "--cluster-identifier", help="name identifier of the cluster"
    )

    if len(args) == 0:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)

    if "upgrade-cluster-version" in args.command:
        upgrade_cluster_version(args)


def run():
    """Calls :func:`main` passing the CLI arguments extracted from
    :obj:`sys.argv`

    This function can be used as entry point to create console scripts with
    setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
