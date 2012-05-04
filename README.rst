Taskmaster
----------

Taskmaster is a simple distributed queue designed for handling large numbers of one-off tasks.

We built this at DISQUS to handle frequent, but uncommon tasks like "migrate this data to a new schema".

Why?
----

You might ask, "Why not use Celery?". Well the answer is simply that normal queueing requires (not literally,
but it'd be painful without) you to buffer all tasks into a central location. This becomes a problem when you
have a large amount of tasks, especially when they contain a large amount of data.

Imagine you have 1 billion tasks, each weighing in at 5k. Thats, uncompressed, at minimum 4 terabytes of storage
required just to keep that around, and gains you very little.

Taskmaster on the other hand is designed to take an resumable iterator, and only pull in a maximum number of
jobs at a time (using standard Python Queue's). This ensures a consistent memory pattern that can scale linearly.

Usage
-----

Create an iterator, and callback::

    # taskmaster/example.py
    def get_jobs(last=0):
        # last_job would be sent if state was resumed
        # from a previous run
        for i in xrange(last, 100000000):
            yield i

    def handle_job(i):
        print "Got %r!" % i


Spawn a master::

    tm-master taskmaster.example:get_jobs --host=0.0.0.0:3050 --key=foobar --size=10000

Spawn slaves::

    tm-slave taskmaster.example:handle_job --host=127.0.0.1:3050 --key=foobar --threads=1

.. note:: All arguments are optional, and will default to localhost with no auth key.