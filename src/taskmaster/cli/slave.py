"""
taskmaster.cli.slave
~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


def run(target, host='0.0.0.0:3050', progressbar=True):
    from taskmaster.consumer import Consumer
    from taskmaster.util import import_target

    host, port = host.split(':')

    target = import_target(target, 'handle_job')

    client = Consumer(host, port, progressbar=progressbar)
    client.start(target)


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--progress", dest="progressbar", action="store_true", default=False)
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-slave <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
