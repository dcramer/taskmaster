"""
taskmaster.progressbar
~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from __future__ import absolute_import

from progressbar import ProgressBar, UnknownLength, Counter, Timer
from progressbar.widgets import Widget


class Speed(Widget):
    'Widget for showing the rate.'

    format = 'Rate:  %6.2f/s'

    def __init__(self):
        self.startval = 0

    def update(self, pbar):
        'Updates the widget with the current SI prefixed speed.'

        if self.startval is 0:
            self.startval = pbar.currval
            return 'Rate:  --/s'

        speed = (pbar.currval - self.startval) / pbar.seconds_elapsed

        return self.format % speed
