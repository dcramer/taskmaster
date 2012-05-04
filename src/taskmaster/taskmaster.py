import sys
import time
from cPickle import dumps, loads
from os import path, unlink

from taskmaster.workers import ThreadPool


class Taskmaster(object):
    def __init__(self, callback, queryset, state_file=None, qs_kwargs=None, node='1/1', progress=True):
        if not state_file:
            callback_file = sys.modules[callback.__module__].__file__
            state_file = path.join(path.dirname(callback_file), '%s.node%s.state' % (path.basename(callback_file), node.replace('/', '-')))

        if qs_kwargs is None:
            qs_kwargs = {}

        self.nodestr = node
        self.node, self.total_nodes = map(int, node.split('/', 1))
        self.node -= 1

        self.callback = callback
        self.state_file = state_file
        self.queryset = queryset
        self.qs_kwargs = qs_kwargs

        self.progress = progress

    def read_state(self):
        if path.exists(self.state_file):
            print "Reading previous state from %r" % self.state_file
            with open(self.state_file, 'r') as fp:
                data = fp.read()
                if not data:
                    return {}
                try:
                    return loads(data)
                except Exception, e:
                    print e
        return {}

    def state_writer(self, id_state):
        def cleanup(last_id):
            for id_val, done in id_state.items():
                if done and id_val <= last_id:
                    id_state.pop(id_val, None)

        with open(self.state_file, 'w') as state_fp:
            i = 0
            while True:
                try:
                    # we sort by lowest unprocessed id first, then highest processed id
                    last_job = sorted(id_state.items(), key=lambda x: (x[1], -x[0] if x[1] else x[0]))[0][0]
                except IndexError:
                    time.sleep(0.1)
                    continue

                state_fp.seek(0)
                state_fp.write(dumps(last_job))

                cleanup(last_job)

                i += 1
                if self.progress:
                    self.pbar.update(i)

    def handle(self, obj, id_state):
        if obj.pk % self.total_nodes != self.node:
            return

        id_state[obj.pk] = 0
        try:
            self.callback(obj)
        finally:
            id_state[obj.pk] = 1

    def reset(self):
        if path.exists(self.state_file):
            unlink(self.state_file)

    def get_pool(self, workers=1):
        return ThreadPool(workers)

    def put_job(self, pool, func, *args):
        pool.spawn_n(func, *args)

    def run(self, workers=1):
        id_state = {
            # stores a map of object ids to an int value representing if
            # they've completed yet
            # obj.pk: 1/0
        }

        queryset = self.queryset
        qs_kwargs = self.qs_kwargs

        state = self.read_state()

        if state.get('last_id'):
            qs_kwargs['min_id'] = max(int(state['last_id']), qs_kwargs.get('min_id', 0))

        pool = self.get_pool(workers)

        widgets = ['Status: ', Counter(), ' | ', Speed(), ' | ', Timer()]

        print "Starting workers for thread=%r (node=%s) at min_id=%s" % (
            thread.get_ident(), self.nodestr, qs_kwargs.get('min_id') or 0)
        state_writer = Thread(target=self.state_writer, kwargs={
            'id_state': id_state,
        })
        state_writer.daemon = True
        state_writer.start()

        if self.progress:
            self.pbar = ProgressBar(widgets=widgets, maxval=UnknownLength)
            self.pbar.start()

        for obj in RangeQuerySetWrapper(queryset, sorted=True, **qs_kwargs):
            self.put_job(pool, self.handle, obj, id_state)

        pool.waitall()
        state_writer.join(1)

        if self.progress:
            self.pbar.finish()