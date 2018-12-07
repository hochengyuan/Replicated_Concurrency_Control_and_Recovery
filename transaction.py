'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

class Transaction:
    '''
    This class describes the transaction. A transaction is first initiated by user from stdin,
    and received instructions from transaction managerself.
    Variable table:
    - id: the id of this transaction
    - timeStamp: the timeStamp when this transaction is initiated. A transaction is younger than another
        if and only if self.timeStamp > other.timeStamp
    - readOnly: describe whether this transaction is read-only
    - abort: ture if this transaction has been aborted
    - changes: all changes to the sites made by this transaction
    - log: all results read by this transaction
    - reason: store the reason why this transaction is aborted
    '''

    def __init__(self, id, timeStamp, readOnly=False):
        self.id = id
        self.timeStamp = timeStamp
        self.readOnly = readOnly
        self.abort = False
        self.changes = []
        self.log = []
        self.reason = ''


    def makeVarsCopy(self, sites):
        '''
        Called by transaction manager when this transaction is read-onlyself.
        To make a multi-version image of all readable vars in all online sites.
        '''
        self.varsCopy = [None] * 21
        for i in range(1, 21):
            for j in range(1, 11):
                site = sites[j]
                if site.online and site.exist(i) and site.vars[i].canRead:
                    self.varsCopy[i] = site.vars[i].value
                    break


    def acquireWriteLock(self, sites, varId):
        '''
        Acquire a write lock from a var
        '''
        for i in range(1, 11):
            site = sites[i]
            if site.online and site.exist(varId):
                var = site.vars[varId]
                var.addWriteLock(self)


    def canWrite(self, sites, varId):
        '''
        To check whether this transaction can write to a particular var.
        '''
        for i in range(1, 11):
            site = sites[i]
            if site.online and site.exist(varId):
                var = site.vars[varId]
                if self.id not in var.getPrioLock():
                    return False
        return True


    def canRead(self, sites, varId):
        '''
        To check whether this transaction can read to a particular var.
        '''
        for i in range(1, 11):
            site = sites[i]
            if site.online and site.exist(varId):
                var = site.vars[varId]
                if self.id not in var.getPrioLock():
                    return False
        return True


    def write(self, sites, varId, value):
        '''
        Make changes to a var in all online sites
        '''
        target = []
        for i in range(1, 11):
            if sites[i].online:
                target.append(i)
        self.changes.append([varId, value, target])


    def read(self, sites, varId):
        '''
        Read a readable vars in any online site
        '''
        for i in range(1, 11):
            site = sites[i]
            if site.online and site.exist(varId) and site.vars[varId].canRead:
                self.log.append((varId, site.vars[varId].value))
                break


    def acquireReadLock(self, sites, varId):
        '''
        Acquire a read lock from a var
        '''
        for i in range(1, 11):
            site = sites[i]
            if site.online and site.exist(varId):
                var = site.vars[varId]
                var.addWriteLock(self)


    def commit(self, sites):
        '''
        Commit all changes to target site.
        Return all read results to the transaction manager.
        Release all locks of this transaction.
        '''
        for key, val, target in self.changes:
            for i in target:
                site = sites[i]
                if site.online and site.exist(key):
                    var = site.vars[key]
                    var.setValue(val)

        for i in range(1, 11):
            site = sites[i]
            for var in site.vars.values():
                var.releaseLock(self)

        res = []
        for id, val in self.log:
            res.append('x{}: {}'.format(id, val))
        return '\n'.join(res)


    def setAbort(self, sites, ops, reason):
        '''
        Set this transaction aborted.
        '''
        self.abort = True
        self.reason = reason
        for i in range(len(ops)):
            if ops[i][1] == self:
                del ops[i]

        for i in range(1, 11):
            site = sites[i]
            for var in site.vars.values():
                var.releaseLock(self)
