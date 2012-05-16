"""
taskmaster.consumer
~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import cPickle as pickle
import gevent
from gevent_zeromq import zmq
from gevent.queue import Queue
from taskmaster.util import import_target


class Worker(object):
    def __init__(self, consumer, target):
        self.consumer = consumer
        self.target = target

    def run(self):
        self.started = True

        while self.started:
            gevent.sleep(0)

            try:
                job_id, job = self.consumer.get_job()
                self.target(job)
            except KeyboardInterrupt:
                return
            finally:
                self.consumer.task_done()


class Client(object):
    def __init__(self, address, timeout=2500, retries=3):
        self.address = address
        self.timeout = timeout
        self.retries = retries

        self.context = zmq.Context(1)
        self.poller = zmq.Poller()
        self.client = None

    def reconnect(self):
        if self.client:
            self.poller.unregister(self.client)
            self.client.close()
            print "Reconnecting to server on %r" % self.address
        else:
            print "Connecting to server on %r" % self.address

        self.client = self.context.socket(zmq.REQ)
        self.client.setsockopt(zmq.LINGER, 0)
        self.client.connect(self.address)
        self.poller.register(self.client, zmq.POLLIN)

    def send(self, cmd, data=''):
        request = [cmd, data]
        retries = self.retries
        reply = None

        while retries > 0:
            gevent.sleep(0)

            self.client.send_multipart(request)
            try:
                items = self.poller.poll(self.timeout)
            except KeyboardInterrupt:
                break  # interrupted

            if items:
                reply = self.recv()
                break
            else:
                if retries:
                    self.reconnect()
                else:
                    break
                retries -= 1

        return reply

    def recv(self):
        reply = self.client.recv_multipart()

        assert len(reply) == 2

        return reply

    def destroy(self):
        if self.client:
            self.poller.unregister(self.client)
            self.client.setsockopt(zmq.LINGER, 0)
            self.client.close()
        self.context.destroy()


class Consumer(object):
    def __init__(self, client, target, progressbar=True):
        if isinstance(target, basestring):
            target = import_target(target, 'handle_job')

        self.client = client
        self.target = target
        self.queue = Queue(maxsize=1)
        if progressbar:
            self.pbar = self.get_progressbar()
        else:
            self.pbar = None

        self._wants_job = False

    def get_progressbar(self):
        from taskmaster.progressbar import Counter, Speed, Timer, ProgressBar, UnknownLength

        widgets = ['Tasks Completed: ', Counter(), ' | ', Speed(), ' | ', Timer()]

        pbar = ProgressBar(widgets=widgets, maxval=UnknownLength)

        return pbar

    def get_job(self):
        self._wants_job = True

        return self.queue.get()

    def task_done(self):
        if self.pbar:
            self.pbar.update(self.tasks_completed)
        self.tasks_completed += 1
        # self.client.send('DONE')

    def start(self):
        self.started = True
        self.tasks_completed = 0

        self.client.reconnect()

        worker = Worker(self, self.target)
        gevent.spawn(worker.run)

        if self.pbar:
            self.pbar.start()

        while self.started:
            gevent.sleep(0)

            # If the queue has items in it, we just loop
            if not self._wants_job:
                continue

            reply = self.client.send('GET')
            if not reply:
                break

            cmd, data = reply
            # Reply can be "WAIT", "OK", or "ERROR"
            if cmd == 'OK':
                self._wants_job = False
                job = pickle.loads(data)
                self.queue.put(job)
            elif cmd == 'QUIT':
                break

        self.shutdown()

    def shutdown(self):
        if not self.started:
            return
        self.started = False
        if self.pbar:
            self.pbar.finish()
        self.client.destroy()
