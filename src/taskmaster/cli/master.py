"""
taskmaster.cli.master
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from taskmaster.util import parse_options
from taskmaster.constants import DEFAULT_LOG_LEVEL, DEFAULT_ADDRESS, \
  DEFAULT_BUFFER_SIZE


def run(target, kwargs=None, reset=False, size=DEFAULT_BUFFER_SIZE, address=DEFAULT_ADDRESS, log_level=DEFAULT_LOG_LEVEL):
    from taskmaster.server import Server, Controller

    server = Server(address, size=size, log_level=log_level)

    controller = Controller(server, target, kwargs=kwargs, log_level=log_level)
    if reset:
        controller.reset()
    controller.start()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--address", dest="address", default=DEFAULT_ADDRESS)
    parser.add_option("--size", dest="size", default=DEFAULT_BUFFER_SIZE, type=int)
    parser.add_option("--reset", dest="reset", default=False, action='store_true')
    parser.add_option("--log-level", dest="log_level", default=DEFAULT_LOG_LEVEL)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        print 'Usage: tm-master <callback> [key=value, key2=value2]'
        sys.exit(1)
    sys.exit(run(args[0], parse_options(args[1:]), **options.__dict__))

if __name__ == '__main__':
    main()
