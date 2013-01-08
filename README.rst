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

Taskmaster on the other hand is designed to take a resumable iterator, and only pull in a maximum number of
jobs at a time (using standard Python Queue's). This ensures a consistent memory pattern that can scale linearly.

Requirements
------------

Requirements **should** be handled by setuptools, but if they are not, you will need the following Python packages:

* progressbar
* pyzmq (zeromq)
* gevent
* gevent_zeromq


A note on Gevent
----------------

Being that Taskmaster uses gevent for both its iterator task (master) and its consumers, your application will need
to correctly implement non-blocking gevent compatible callers. In most cases this won't be a problem, but if you're
using the network you'll need to look for a compatible library for your adapter. For example, there is an alternative
version of ``psycopg2`` designed for gevent called ``gevent-psycopg2``.

Usage
-----

Create an iterator, and callback::

    # taskmaster/example.py
    def get_jobs(last=0):
        # last would be sent if state was resumed
        # from a previous run
        for i in xrange(last, 100000000):
            # jobs yielded must be serializeable with pickle
            yield i

    def handle_job(i):
        # this **must** be idempotent, as resuming the process may execute a job
        # that had already been run
        print "Got %r!" % i


Spawn a master::

    $ tm-master taskmaster.example

You can also pass keyword arguments for the master::

    $ tm-master taskmaster.example argument=value

Spawn a slave::

    $ tm-slave taskmaster.example

Or spawn 8 slaves (each containing a threadpool)::

    $ tm-spawn taskmaster.example 8

Dont like the magical function discover for master/slave? Specify your own targets::

    $ tm-master taskmaster.example:get_jobs
    $ tm-slave taskmaster.example:handle_job

Maybe you simply need to run things on the same server?

::

    $ tm-run taskmaster/example.py 8

.. note:: All arguments are optional, and the address will default to ``tcp://0.0.0.0:3050``.