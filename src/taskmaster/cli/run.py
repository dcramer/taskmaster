"""
taskmaster.cli.run
~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from multiprocessing import Process
from taskmaster.cli.spawn import run as run_spawn
from taskmaster.cli.master import run as run_master
from taskmaster.constants import DEFAULT_LOG_LEVEL, DEFAULT_ADDRESS, \
  DEFAULT_ITERATOR_TARGET, DEFAULT_CALLBACK_TARGET, DEFAULT_BUFFER_SIZE
from taskmaster.util import parse_options


def run(get_jobs_target, handle_job_target, procs, kwargs=None, log_level=DEFAULT_LOG_LEVEL,
        address=DEFAULT_ADDRESS, reset=False, size=DEFAULT_BUFFER_SIZE):
    pool = [
        Process(target=run_master, args=[get_jobs_target], kwargs={
            'log_level': log_level,
            'address': address,
            'size': size,
            'reset': reset,
            'kwargs': kwargs,
        }),
        Process(target=run_spawn, args=[handle_job_target, procs], kwargs={
            'log_level': log_level,
            'address': address,
            'progressbar': False,
        }),
    ]

    for p in pool:
        p.start()

    for p in (p for p in pool if p.is_alive()):
        p.join(0)


def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--address", dest="address", default=DEFAULT_ADDRESS)
    parser.add_option("--size", dest="size", default='10000', type=int)
    parser.add_option("--reset", dest="reset", default=False, action='store_true')
    parser.add_option("--log-level", dest="log_level", default=DEFAULT_LOG_LEVEL)
    parser.add_option("--get-jobs-callback", dest="get_jobs_target", default=DEFAULT_ITERATOR_TARGET)
    parser.add_option("--handle-job-callback", dest="handle_job_target", default=DEFAULT_CALLBACK_TARGET)
    (options, args) = parser.parse_args()
    if len(args) != 2:
        print 'Usage: tm-run <script> <processes> [key=value, key2=value2]'
        sys.exit(1)

    kwargs = options.__dict__.copy()

    script_name = args[0]
    handle_job_target = script_name + ':' + kwargs.pop('handle_job_target')
    get_jobs_target = script_name + ':' + kwargs.pop('get_jobs_target')

    sys.exit(run(
        get_jobs_target=get_jobs_target,
        handle_job_target=handle_job_target,
        procs=int(args[1]),
        kwargs=parse_options(args[2:]),
        **kwargs
    ))

if __name__ == '__main__':
    main()
