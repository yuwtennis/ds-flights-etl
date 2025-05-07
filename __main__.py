""" Top level main module """
import sys
from dsflightsdpr.client import run


def main():
    """ Main function """
    run(sys.argv, True)


if __name__ == '__main__':
    main()
