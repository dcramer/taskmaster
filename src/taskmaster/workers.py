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
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        self.running = True
        while self.running:
            try:
                func, args, kwargs = self.queue.get_nowait()
            except Empty:
                time.sleep(0.1)
                continue

            try:
                func(*args, **kwargs)
            except KeyboardInterrupt:
                return
            finally:
                self.queue.task_done()


class ThreadPool(object):
    def __init__(self, queue, size=10):
        self.workers = []
        for worker in xrange(size):
            self.workers.append(Worker(queue))

        for worker in self.workers:
            worker.start()

    def join(self):
        for worker in self.workers:
            worker.running = False
            worker.join()
