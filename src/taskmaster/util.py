"""
taskmaster.util
~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import imp
import logging
import sys
from os.path import exists


def import_target(target, default=None):
    """
    >>> import_target('foo.bar:blah', 'get_jobs')
    <function foo.bar.blah>

    >>> import_target('foo.bar', 'get_jobs')
    <function foo.bar.get_jobs>

    >>> import_target('foo.bar:get_jobs')
    <function foo.bar.get_jobs>

    >>> import_target('foo/bar.py:get_jobs')
    <function get_jobs>
    """
    if ':' not in target:
        target += ':%s' % default

    path, func_name = target.split(':', 1)

    if exists(path):
        module_name = path.rsplit('/', 1)[-1].split('.', 1)[0]
        module = imp.new_module(module_name)
        module.__file__ = path
        try:
            execfile(path, module.__dict__)
        except IOError, e:
            e.strerror = 'Unable to load file (%s)' % e.strerror
            raise
        sys.modules[module_name] = module
    elif '/' in path:
        raise ValueError('File not found: %r' % path)
    else:
        module = __import__(path, {}, {}, [func_name], -1)

    callback = getattr(module, func_name)

    return callback


def get_logger(inst, log_level='INFO'):
    logger = logging.getLogger('%s.%s[%s]' % (inst.__module__, type(inst).__name__, id(inst)))
    logger.setLevel(getattr(logging, log_level))
    logger.addHandler(logging.StreamHandler())
    return logger


def parse_options(args):
    return dict(a.split('=', 1) for a in args)
