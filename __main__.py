""" Top level main module """
import sys
from dsflightsetl.client import run

def main():
    """ Main function """
    run(sys.argv, False)

if __name__ == '__main__':
    main()
