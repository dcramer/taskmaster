"""
taskmaster.cli.slave
~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


def run(target, address='tcp://127.0.0.1:3050', progressbar=True):
    from taskmaster.client import Consumer, Client

    client = Client(address)

    consumer = Consumer(client, target, progressbar=progressbar)
    consumer.start()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--address", dest="address", default='tcp://127.0.0.1:3050')
    parser.add_option("--no-progress", dest="progressbar", action="store_false", default=True)
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-slave <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
