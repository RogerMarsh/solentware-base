# callthreadqueue.py
# Copyright 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Run methods placed on a queue in a thread from a pool of size one.

List of classes:

MethodThread

List of functions:

None

"""
import queue
import threading


class CallThreadQueue(object):
    """Provide a queue for methods to be run by a thread.

    The maximum size of the queue is one and there is one thread serving it.

    Methods added:

    __call_method
    put_method
    
    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self):
        """Create the queue andstart the thread."""
        super(CallThreadQueue, self).__init__()
        self.queue = queue.Queue(maxsize=1)
        threading.Thread(target=self.__call_method, daemon=True).start()

    def __call_method(self):
        """Get method from queue, run it, and then wait for next method"""
        while True:
            try:
                method, args, kwargs = self.queue.get()
            except:
                self.queue.task_done()
                self.queue = None
                break
            method(*args, **kwargs)
            self.queue.task_done()

    def put_method(self, method, args=(), kwargs={}):
        """Append the method and it's arguments to the queue."""
        self.queue.put((method, args, kwargs))
