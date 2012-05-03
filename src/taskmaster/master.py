"""
taskmaster.master
~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing.managers import BaseManager
import Queue


def run(size=10000, host='0.0.0.0:3050', key='taskmaster'):
    class QueueManager(BaseManager):
        pass

    host, port = host.split(':')

    queue = Queue.Queue(max_size=size)

    QueueManager.register('get_queue', callable=lambda: queue)

    m = QueueManager(address=(host, int(port)), key=key)
    s = m.get_server()
    s.serve_forever()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--size", dest="size", default='10000', type=int)
    parser.add_option("--key", dest="key", default='taskmaster')
    (options, args) = parser.parse_args()
    sys.exit(run(**options.__dict__))

if __name__ == '__main__':
    main()
