"""
taskmaster.util
~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""


def import_target(target, default=None):
    """
    >>> import_target('foo.bar:blah', 'get_jobs')
    <function foo.bar.blah>

    >>> import_target('foo.bar', 'get_jobs')
    <function foo.bar.get_jobs>

    >>> import_target('foo.bar:get_jobs')
    <function foo.bar.get_jobs>
    """
    if ':' not in target:
        target += ':%s' % default
    else:
        raise ValueError('target must be in form of `path.to.module:function_name`')

    mod_path, func_name = target.split(':', 1)

    module = __import__(mod_path, {}, {}, [func_name], -1)
    callback = getattr(module, func_name)

    return callback
