'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

import sys

from manager import Manager

if __name__ == '__main__':
    '''
    This is an interface provided to user to add instructions to the transaction manager.
    '''
    tm = Manager()
    timeStamp = 0
    line = sys.stdin.readline()
    while line:
        tm.parse(line, timeStamp)
        timeStamp += 1
        line = sys.stdin.readline()
