"""
taskmaster.consumer
~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import cPickle as pickle
import gevent
from gevent_zeromq import zmq
from gevent.queue import Queue, Empty


class Worker(object):
    def __init__(self, client, target):
        self.client = client
        self.target = target

    def run(self):
        self.running = True
        while self.running:
            job_id, job = self.client.get_job()

            try:
                self.target(job)
            except KeyboardInterrupt:
                return
            finally:
                self.client.task_done()


class Consumer(object):
    def __init__(self, host, port, progressbar=True, request_timeout=2500):
        self.daemon = True
        self.started = False
        self.address = 'tcp://%s:%s' % (host, port)
        self.request_timeout = request_timeout
        self.queue = Queue()
        self._wants_job = False

        if progressbar:
            self.pbar = type(self).get_progressbar()
        else:
            self.pbar = None

    @classmethod
    def get_progressbar(cls):
        from taskmaster.progressbar import Counter, Speed, Timer, ProgressBar, UnknownLength

        widgets = ['Current Job: ', Counter(), ' | ', Speed(), ' | ', Timer()]

        pbar = ProgressBar(widgets=widgets, maxval=UnknownLength)

        return pbar

    def start(self, target):
        self.started = True
        self.tasks_completed = 0

        self.context = context = zmq.Context(1)
        self.client = client = context.socket(zmq.REQ)
        self.poll = poll = zmq.Poller()

        client.connect(self.address)
        poll.register(client, zmq.POLLIN)

        worker = Worker(self, target)
        gevent.spawn(worker.run)

        print "Connecting to server on %r" % self.address

        if self.pbar:
            self.pbar.start()

        while True:
            # If the queue has items in it, we just loop
            if not self._wants_job:
                gevent.sleep(0)
                continue

            client.send('GET')
            socks = dict(poll.poll(self.request_timeout))
            if socks.get(client) != zmq.POLLIN:
                # server connection closed
                break

            reply = client.recv()
            if not reply:
                break

            # Reply can be "WAIT", "OK", or "ERROR"
            if reply.startswith('OK '):
                self._wants_job = False
                job = pickle.loads(reply[3:])
                self.queue.put(job)

        self.shutdown()

    def get_job(self):
        self._wants_job = True

        return self.queue.get()

    def task_done(self):
        self.tasks_completed += 1
        if self.pbar:
            self.pbar.update(self.tasks_completed)

    def shutdown(self):
        if not self.started:
            return
        self.poll.unregister(self.client)
        self.client.close()
        self.context.term()
        if self.pbar:
            self.pbar.finish()
        self.started = False
