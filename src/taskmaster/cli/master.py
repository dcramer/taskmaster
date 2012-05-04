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
        self.started = False
        self.queue = Queue.Queue(maxsize=size)

        QueueManager.register('get_queue', callable=lambda: self.queue)

        self.manager = QueueManager(address=(host, int(port)), authkey=authkey)

    def run(self):
        self.started = True
        server = self.manager.get_server()
        print "Taskmaster server running on %r" % ':'.join(map(str, server.address))
        server.serve_forever()

    def put_job(self, job):
        self.queue.put(job)

    def first_job(self):
        return self.queue.queue[0]

    def has_work(self):
        return not self.queue.empty()

    def is_alive(self):
        return self.started

    def shutdown(self):
        # TODO:
        # if self.started:
        #     self.manager.shutdown()
        self.started = False


def run(target, reset=False, size=10000, host='0.0.0.0:3050', key='taskmaster'):
    host, port = host.split(':')

    server = QueueServer(host, int(port), size=size, authkey=key)

    controller = Controller(server, target)
    if reset:
        controller.reset()
    controller.start()


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--size", dest="size", default='10000', type=int)
    parser.add_option("--key", dest="key", default='taskmaster')
    parser.add_option("--reset", dest="reset", default=False, action='store_true')
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-master <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
