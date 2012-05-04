"""
taskmaster.util
~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""
import imp
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
    else:
        raise ValueError('target must be in form of `path.to.module:function_name`')

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
    else:
        module = __import__(path, {}, {}, [func_name], -1)

    callback = getattr(module, func_name)

    return callback
