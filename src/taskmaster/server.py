"""
taskmaster.controller
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import cPickle as pickle
import gevent
import sys
from gevent_zeromq import zmq
from gevent.queue import Queue, Empty
from os import path, unlink
from taskmaster.util import import_target


class Server(object):
    def __init__(self, address, size=None):
        self.daemon = True
        self.started = False
        self.size = size
        self.queue = Queue(maxsize=size)
        self.address = address

        self.context = zmq.Context(1)
        self.server = None

    def send(self, cmd, data=''):
        self.server.send_multipart([cmd, data])

    def recv(self):
        reply = self.server.recv_multipart()

        assert len(reply) == 2

        return reply

    def bind(self):
        if self.server:
            self.server.close()

        print "Taskmaster binding to %r" % self.address
        self.server = self.context.socket(zmq.REP)
        self.server.bind(self.address)

    def start(self):
        self.started = True

        self.bind()

        while self.started:
            gevent.sleep(0)
            cmd, data = self.recv()
            if cmd == 'GET':
                if not self.has_work():
                    self.send('QUIT')
                    continue

                try:
                    job = self.queue.get_nowait()
                except Empty:
                    self.send('WAIT')
                    continue

                self.send('OK', pickle.dumps(job))

            elif cmd == 'DONE':
                self.queue.task_done()
                if self.has_work():
                    self.send('OK')
                else:
                    self.send('QUIT')

            else:
                self.send('ERROR', 'Unrecognized command')

        self.shutdown()

    def put_job(self, job):
        return self.queue.put(job)

    def first_job(self):
        return self.queue.queue[0]

    def get_current_size(self):
        return self.queue.qsize()

    def get_max_size(self):
        return self.size

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


class Controller(object):
    def __init__(self, server, target, state_file=None, progressbar=True):
        if isinstance(target, basestring):
            target = import_target(target, 'get_jobs')

        if not state_file:
            target_file = sys.modules[target.__module__].__file__
            state_file = path.join(path.dirname(target_file),
                '%s.state' % (path.basename(target_file),))

        self.server = server
        self.target = target
        self.state_file = state_file
        if progressbar:
            self.pbar = self.get_progressbar()
        else:
            self.pbar = None

    def get_progressbar(self):
        from taskmaster.progressbar import Counter, Speed, Timer, ProgressBar, UnknownLength, Value

        sizelen = len(str(self.server.size))
        format = 'In-Queue: %%-%ds / %%-%ds' % (sizelen, sizelen)

        queue_size = Value(callback=lambda x: format % (self.server.get_current_size(), self.server.get_max_size()))

        widgets = ['Completed Tasks: ', Counter(), ' | ', queue_size, ' | ', Speed(), ' | ', Timer()]

        pbar = ProgressBar(widgets=widgets, maxval=UnknownLength)

        return pbar

    def read_state(self):
        if path.exists(self.state_file):
            print "Reading previous state from %r" % self.state_file
            with open(self.state_file, 'r') as fp:
                try:
                    return pickle.load(fp)
                except EOFError:
                    pass
                except Exception, e:
                    print "There was an error reading from state file. Ignoring and continuing without."
                    import traceback
                    traceback.print_exc()
                    print e
        return {}

    def update_state(self, job_id, job, fp=None):
        last_job_id = getattr(self, '_last_job_id', None)

        if self.pbar:
            self.pbar.update(job_id)

        if job_id == last_job_id:
            return

        if not job:
            return

        last_job_id = job_id

        data = {
            'job': job,
            'job_id': job_id,
        }
        if not fp:
            with open(self.state_file, 'w') as fp:
                pickle.dump(data, fp)
        else:
            fp.seek(0)
            pickle.dump(data, fp)

    def state_writer(self):
        with open(self.state_file, 'w') as fp:
            while self.server.is_alive():
                gevent.sleep(0)

                try:
                    job_id, job = self.server.first_job()
                except IndexError:
                    self.update_state(None, None, fp)
                    continue

                self.update_state(job_id, job, fp)

    def reset(self):
        if path.exists(self.state_file):
            unlink(self.state_file)

    def start(self):
        kwargs = {}
        last_job = self.read_state()
        if last_job:
            kwargs['last'] = last_job['job']
            start_id = last_job['job_id']
        else:
            start_id = 0

        gevent.spawn(self.server.start)

        gevent.sleep(0)

        if self.pbar:
            self.pbar.start()
            self.pbar.update(start_id)

        state_writer = gevent.spawn(self.state_writer)

        job_id, job = (None, None)
        for job_id, job in enumerate(self.target(**kwargs), start_id):
            self.server.put_job((job_id, job))
            gevent.sleep(0)

        while self.server.has_work():
            gevent.sleep(0)

        # Give clients a few seconds to receive a DONE message
        gevent.sleep(3)

        self.server.shutdown()
        state_writer.join(1)

        self.update_state(job_id, job)

        if self.pbar:
            self.pbar.finish()
