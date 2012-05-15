"""
taskmaster.example
~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


def get_jobs(last=0):
    # last_job would be sent if state was resumed
    # from a previous run
    for i in xrange(last, 10000):
        yield i


def handle_job(i):
    print "Got %r!" % i
