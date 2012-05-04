Taskmaster
----------

**This is under development, and this README reflects the potential API**

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


.. note:: All arguments are optional