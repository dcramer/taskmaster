"""
taskmaster.workers
~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import time
from threading import Thread
from Queue import Empty


class Worker(Thread):
    def __init__(self, queue, target):
        Thread.__init__(self)
        self.queue = queue
        self.target = target

    def run(self):
        self.running = True
        while self.running:
            try:
                job_id, job = self.queue.get_nowait()
            except Empty:
                time.sleep(0)
                continue
            except EOFError:
                return

            try:
                self.target(job)
            except KeyboardInterrupt:
                return
            finally:
                try:
                    self.queue.task_done()
                except EOFError:
                    return


class ThreadPool(object):
    def __init__(self, queue, target, size=10):
        self.target = target
        self.workers = []
        for worker in xrange(size):
            self.workers.append(Worker(queue, target))

        for worker in self.workers:
            worker.start()

    def is_alive(self):
        return any(w.is_alive() for w in self.workers)

    def join(self, nowait=False):
        for worker in (w for w in self.workers if w.is_alive()):
            if nowait:
                worker.running = False
            worker.join(0)
            time.sleep(0)
