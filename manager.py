'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

from collections import deque, defaultdict

from server import Site
from transaction import Transaction

class Manager:
    '''
    Transaction manager.
    Variable table:
    - sites: store information of all sites.
    - curTrans: all transactions initiated by user from stdin.
    - ops: describes the wait-list for all instructions.
    '''

    def __init__(self):
        self.sites = [None] + [Site(i) for i in range(1, 11)]
        self.curTrans = {}
        self.ops = deque()


    def dumpAll(self):
        '''
        Called by user, return all info of all sites and vars.
        '''
        res = []
        for i in range(1, 11):
            site = self.sites[i]
            res.append('site {} - {}'.format(i, site.dump()))
        return '\n'.join(res)


    def run(self):
        '''
        Check whether the instruction in wait list hold all the lock required by the system,
        if so, run it.
        '''
        flag = True
        while flag and self.ops:
            flag = False
            peek = self.ops[0]
            if peek[1].abort:
                flag = True
                self.ops.popleft()
            if peek[0] == 'w':
                _, trans, varId, value = peek
                if trans.canWrite(self.sites, varId):
                    trans.write(self.sites, varId, value)
                    flag = True
                    self.ops.popleft()
            elif peek[0] == 'r':
                _, trans, varId = peek
                if trans.canRead(self.sites, varId):
                    trans.read(self.sites, varId)
                    flag = True
                    self.ops.popleft()


    def detDeadlock(self):
        '''
        Deadlock detection function. Use the same cycle detection algorithm with topoSort.
        If a deadlock found, return all possible critical nodes.
        '''
        nextMap = defaultdict(set)

        for i in range(1, 11):
            site = self.sites[i]
            for var in site.vars.values():
                for lock in var.locks.values():
                    temp = set()
                    for x in lock.prev:
                        temp.add(x.id)
                    nextMap[lock.id] |= temp

        if not nextMap: return

        flag = True
        for key in nextMap:
            if len(nextMap[key]) == 0:
                flag = False
                break

        if flag:
            target, age = None, -1
            for id in nextMap:
                if self.curTrans[id].timeStamp > age:
                    age = self.curTrans[id].timeStamp
                    target = self.curTrans[id]

            target.setAbort(self.sites, self.ops, 'deadlock')


    def formalize(self, str):
        '''
        Formalize the input from stdin.
        '''
        res = str.replace(' ','').lower().strip()
        return res


    def dumpBySite(self, varId):
        '''
        Return the value of the var from all sites.
        '''
        res = []
        for i in range(1, 11):
            site = self.sites[i]
            if site.exist(varId):
                res.append('site {} - x{}: {}'.format(i, varId, site.vars[varId].value))
        return res


    def parse(self, op, timeStamp):
        '''
        Parse the inputdate from user.
        '''

        self.detDeadlock()
        self.run()

        op = self.formalize(op)
        # print(op)
        if op.startswith('beginro'):
            transId = int(op[op.find('(') + 2 : op.find(')')])
            trans = Transaction(transId, timeStamp, readOnly=True)
            trans.makeVarsCopy(self.sites)
            self.curTrans[transId] = trans
        elif op.startswith('begin'):
            transId = int(op[op.find('(') + 2 : op.find(')')])
            trans = Transaction(transId, timeStamp)
            self.curTrans[transId] = trans
        elif op.startswith('recover'):
            siteId = int(op[op.find('(') + 1 : op.find(')')])
            site = self.sites[siteId]
            site.recover(self.sites)
        elif op.startswith('r'):
            args = op[op.find('(') + 1 : op.find(')')].split(',')
            transId, varId = int(args[0][1:]), int(args[1][1:])
            trans = self.curTrans[transId]
            if trans.readOnly:
                if trans.varsCopy[varId] is not None:
                    print('x{}: {}'.format(varId, trans.varsCopy[varId]))
                else:
                    print('T{} cannot read x{} at this time'.format(transId, varId))
            else:
                if not trans.abort:
                    trans.acquireReadLock(self.sites, varId)
                    self.ops.append(['r', trans, varId])
        elif op.startswith('w'):
            args = op[op.find('(') + 1 : op.find(')')].split(',')
            transId, varId, value = int(args[0][1:]), int(args[1][1:]), int(args[2])
            trans = self.curTrans[transId]
            if not trans.abort:
                trans.acquireWriteLock(self.sites, varId)
                self.ops.append(['w', trans, varId, value])
        elif op.startswith('dump'):
            arg = op[op.find('(') + 1 : op.find(')')]
            if arg == '':
                print(self.dumpAll())
            elif arg.startswith('x'):
                varId = int(arg[1:])
                print('\n'.join(self.dumpBySite(varId)))
            else:
                siteId = int(arg)
                print(self.sites[siteId].dump())
        elif op.startswith('end'):
            transId = int(op[op.find('(') + 2 : op.find(')')])
            trans = self.curTrans[transId]
            if trans.abort:
                print('T{} aborts ({})'.format(transId, trans.reason))
            else:
                log = trans.commit(self.sites)
                if log:
                    print(log)
                print('T{} commits'.format(transId))
        elif op.startswith('fail'):
            siteId = int(op[op.find('(') + 1 : op.find(')')])
            site = self.sites[siteId]
            abortSet = site.fail()
            for transId in abortSet:
                if transId in self.curTrans:
                    self.curTrans[transId].setAbort(self.sites, self.ops, 'site down')

        # print('-----before------')
        # print(self.ops)
        # print('----------------')
        # print('input: {}, locks: {}'.format(op, self.sites[1].vars[2].locks.keys()))
        self.run()
        # print('-----after------')
        # print(self.ops)
        # print('----------------')
