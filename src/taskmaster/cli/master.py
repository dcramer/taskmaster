"""
taskmaster.cli.master
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


def run(target, reset=False, size=10000, address='tcp://0.0.0.0:3050', log_level='INFO'):
    from taskmaster.server import Server, Controller

    server = Server(address, size=size, log_level=log_level)

    controller = Controller(server, target, log_level=log_level)
    if reset:
        controller.reset()
    controller.start()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--address", dest="address", default='tcp://127.0.0.1:3050')
    parser.add_option("--size", dest="size", default='10000', type=int)
    parser.add_option("--reset", dest="reset", default=False, action='store_true')
    parser.add_option("--log-level", dest="log_level", default='INFO')
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-master <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
