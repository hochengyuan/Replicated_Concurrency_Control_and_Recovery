'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

from variable import Variable

class Site:
    '''
    This class describes the server(database).
    Variable table:
    - id: the id of this site
    - vars: all vars hold by this site
    - online: true if this site is now up.
    '''

    def __init__(self, id):
        self.id = id
        self.vars = {}
        self.online = True
        for i in range(1, 21):
            if i % 2 == 0 or 1 + i % 10 == id:
                self.vars[i] = Variable(i)


    def exist(self, varId):
        '''
        Check whether this site contains a var
        '''
        return varId in self.vars


    def dump(self):
        '''
        Return variable information of this site.
        '''
        res = []
        for i in range(1, 21):
            if self.exist(i):
                res.append('x{}: {}'.format(i, self.vars[i].value))
        return ', '.join(res)


    def fail(self):
        '''
        Set this site down. Release all the locks and return all transactions' id who hold a lock on this site.
        '''
        hashSet = set()
        for var in self.vars.values():
            for lock in var.locks.values():
                hashSet.add(lock.id)

        for var in self.vars.values():
            var.locks = {}

        self.online = False
        return hashSet


    def recover(self, sites):
        '''
        Recover this site.
        '''
        for var in self.vars.values():
            var.canRead = False

        for i in range(1, 21):
            if not self.exist(i): continue
            for j in range(1, 11):
                if j == self.id: continue
                site = sites[j]
                if site.online and site.exist(i) and site.vars[i].canRead and self.vars[i].value == site.vars[i].value:
                    self.vars[i].canRead = True

        self.online = True
