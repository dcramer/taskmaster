"""
taskmaster.slave
~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing.managers import BaseManager


def run(host='0.0.0.0:3050', key='taskmaster'):
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_queue')

    host, port = host.split(':')

    m = QueueManager(address=(host, int(port)), key=key)
    m.connect()
    # queue = m.get_queue()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--key", dest="key", default='taskmaster')
    (options, args) = parser.parse_args()
    sys.exit(run(**options.__dict__))

if __name__ == '__main__':
    main()
