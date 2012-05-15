"""
taskmaster.cli.master
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import cPickle as pickle
import gevent
from gevent_zeromq import zmq
from taskmaster.controller import Controller
from gevent.queue import Queue, Empty, Full


class Server(object):
    def __init__(self, host, port, size=None):
        self.daemon = True
        self.started = False
        self.queue = Queue(maxsize=size)
        self.address = 'tcp://%s:%s' % (host, port)

    def start(self):
        self.started = True
        self.context = context = zmq.Context(1)

        self.server = server = context.socket(zmq.REP)
        server.bind(self.address)

        print "Taskmaster server running on %r" % self.address

        while self.started:
            request = server.recv()
            if request == 'GET':
                try:
                    job = self.queue.get_nowait()
                except Empty:
                    server.send('WAIT')
                    continue

                server.send('OK %s' % (pickle.dumps(job),))
            elif request == 'DONE':
                self.queue.task_done()
                server.send('OK')
            else:
                server.send('ERROR Unrecognized command')

        self.shutdown()

    def put_job(self, job):
        while True:
            try:
                return self.queue.put_nowait(job)
            except Full:
                gevent.sleep(0)

    def first_job(self):
        return self.queue.queue[0]

    def has_work(self):
        return not self.queue.empty()

    def is_alive(self):
        return self.started

    def shutdown(self):
        if not self.started:
            return
        self.server.close()
        self.context.term()
        self.started = False


def run(target, reset=False, size=10000, host='0.0.0.0:3050'):
    host, port = host.split(':')

    server = Server(host, int(port), size=size)

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
    parser.add_option("--reset", dest="reset", default=False, action='store_true')
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print 'Usage: tm-master <callback>'
        sys.exit(1)
    sys.exit(run(args[0], **options.__dict__))

if __name__ == '__main__':
    main()
