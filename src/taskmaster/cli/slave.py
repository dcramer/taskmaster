"""
taskmaster.cli.slave
~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from taskmaster.constants import (
    DEFAULT_ADDRESS,
    DEFAULT_LOG_LEVEL,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
)


def run(target, address=DEFAULT_ADDRESS, progressbar=True,
        log_level=DEFAULT_LOG_LEVEL, retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT):
    from taskmaster.client import Consumer, Client

    client = Client(address, log_level=log_level, retries=retries,
            timeout=timeout)

    consumer = Consumer(client, target, progressbar=progressbar, log_level=log_level)
    consumer.start()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--address", dest="address", default=DEFAULT_ADDRESS)
    parser.add_option("--no-progress", dest="progressbar", action="store_false", default=True)
    parser.add_option("--log-level", dest="log_level", default=DEFAULT_LOG_LEVEL)
    parser.add_option("--retries", dest="retries", default=DEFAULT_RETRIES, type=int)
    parser.add_option("--timeout", dest="timeout", default=DEFAULT_TIMEOUT, type=int)
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-slave <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
