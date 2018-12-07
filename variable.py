'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

from blocking_graph import BlockingGraphElement

class Variable:
    '''
    This class describes the variable in each site(server).
    Variable Table:
    - id: the id of this var
    - value: the value of this var, the init value is 10 times id
    - locks: the lock table of this var, described as a hashmap(dict) in Python
    - canRead: the flag implies whether this var is readable
    '''

    def __init__(self, id):
        self.id = id
        self.value = id * 10
        self.locks = {}
        self.canRead = True


    def addWriteLock(self, trans):
        '''
        Called by a transaction, to add a lock to this var
        '''
        if trans.id not in self.locks:
            self.locks[trans.id] = BlockingGraphElement(trans.id)
        lock = self.locks[trans.id]

        for key, val in self.locks.items():
            if key == lock.id: continue
            lock.prev.add(val)


    def getPrioLock(self):
        '''
        Called by Variable itself, return ids of all transaction who hold a lock of this var
        '''
        res = set()
        for lock in self.locks.values():
            if not lock.prev:
                res.add(lock.id)
        return res


    def setValue(self, value):
        '''
        Update the value of this var
        '''
        self.value = value
        self.canRead = True


    def releaseLock(self, trans):
        '''
        Release all locks acquired by a particular transaction
        '''
        removedLock = None
        if trans.id in self.locks:
            removedLock = self.locks[trans.id]
            del self.locks[trans.id]

        for lock in self.locks.values():
            if removedLock in lock.prev:
                lock.prev.remove(removedLock)
