"""Parses arguments from commandline"""

import argparse
from argparse import Namespace
from typing import Tuple


def parse_args(argv: list) -> Tuple[Namespace, list[str]]:
    """

    :param argv:
    :return: Returns Tuple of known_arg and pipeline_arg

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample_rate", type=float, default=None)

    return parser.parse_known_args(argv)
