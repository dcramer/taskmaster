"""
taskmaster.cli.master
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing.managers import BaseManager
from threading import Thread
from taskmaster.controller import Controller
import Queue


class QueueManager(BaseManager):
    pass


class QueueServer(Thread):
    def __init__(self, host, port, size=None, authkey=None):
        Thread.__init__(self)
        self.daemon = True
        self.server = None
        self.queue = Queue.Queue(maxsize=size)

        QueueManager.register('get_queue', callable=lambda: self.queue)

        self.manager = QueueManager(address=(host, int(port)), authkey=authkey)

    def run(self):
        self.server = self.manager.get_server()
        self.server.serve_forever()

    def put_job(self, job):
        self.queue.put(job)

    def first_job(self):
        return self.queue.queue[0]

    def has_work(self):
        return not self.queue.empty()

    def shutdown(self):
        if self.server:
            self.server.shutdown()


def sample(last=0):
    return xrange(last, 1000000000)


def run(target, size=10000, host='0.0.0.0:3050', key='taskmaster'):
    host, port = host.split(':')

    server = QueueServer(host, int(port), size=size, authkey=key)

    controller = Controller(server, target)
    controller.start()


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
