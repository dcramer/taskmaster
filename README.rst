Taskmaster
----------

**This is under development, and this README reflects the potential API**

Create an iterator::

    # mymodule/job.py
    def get_jobs():
        for i in xrange(100000000):
            yield i

    def handle_job(i):
        print "Got %r!" % i


Spawn a master::

    tm-master mymodule.job:get_jobs --host=0.0.0.0:3050 --key=foobar --size=10000

Spawn slaves::

    tm-slave mymodule.job:handle_job --host=127.0.0.1:3050 --key=foobar
