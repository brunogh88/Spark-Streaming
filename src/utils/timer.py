"""This module contains the Timer class"""
import time


class Timer(object):
    """Timer class"""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.duration = None

    def start(self):
        """Start timer"""
        self.start_time = time.time()

    def end(self):
        """End timer"""
        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.duration = time.strftime("%H:%M:%S", time.gmtime(total_time))
        self.duration = "%f seconds" % round(
            total_time, 2) \
            if self.duration == '00:00:00' else str(self.duration)
        return self.duration
