"""
taskmaster.cli.master
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing.managers import BaseManager
from threading import Thread
import Queue
import time


class QueueServer(Thread):
    def __init__(self, manager):
        Thread.__init__(self)
        self.manager = manager
        self.server = None

    def run(self):
        self.server = self.manager.get_server()
        self.server.serve_forever()

    def shutdown(self):
        if self.server:
            self.server.shutdown()


class QueueManager(BaseManager):
    pass


def sample(last=0):
    return xrange(last, 1000000)


def run(target, size=10000, host='0.0.0.0:3050', key='taskmaster'):
    host, port = host.split(':')

    queue = Queue.Queue(maxsize=size)

    QueueManager.register('get_queue', callable=lambda: queue)

    manager = QueueManager(address=(host, int(port)), authkey=key)
    server = QueueServer(manager)
    server.daemon = True
    server.start()

    try:
        mod_path, func_name = target.split(':', 1)
    except ValueError:
        raise ValueError('target must be in form of `path.to.module:function_name`')

    module = __import__(mod_path, {}, {}, [func_name], -1)
    callback = getattr(module, func_name)

    # last=<last serialized job>
    kwargs = {}

    for job in callback(**kwargs):
        queue.put(job)

    while not Queue.empty():
        time.sleep(0.1)

    server.shutdown()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--size", dest="size", default='10000', type=int)
    parser.add_option("--key", dest="key", default='taskmaster')
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-master <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
