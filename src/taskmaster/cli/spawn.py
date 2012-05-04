"""
taskmaster.cli.spawn
~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing import Process
from taskmaster.cli.slave import run as run_slave


def run(target, procs, **kwargs):
    pool = []
    for n in xrange(procs):
        pool.append(Process(target=run_slave, args=[target], kwargs=kwargs))

    for p in pool:
        p.start()

    for p in (p for p in pool if p.is_alive()):
        p.join(0)


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--host", dest="host", default='0.0.0.0:3050')
    parser.add_option("--key", dest="key", default='taskmaster')
    parser.add_option("--threads", dest="threads", default=1, type=int)
    # parser.add_option("--procs", dest="procs", default=1, type=int)
    (options, args) = parser.parse_args()
    if len(args) != 2:
        print 'Usage: tm-spawn <callback> <processes>'
        sys.exit(1)
    sys.exit(run(args[0], procs=int(args[1]), **options.__dict__))

if __name__ == '__main__':
    main()
