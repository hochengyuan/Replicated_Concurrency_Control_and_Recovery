'''
Author: Le Wang, Chen-Yuan Ho
Date: December 6, 2018 (Final Vesrion)
'''

class BlockingGraphElement:
    '''
    The node of the blocking graph to detect deadlock.
    Variable talbe:
    - id: the id of this node (describes which transaction add this node).
    - prev: the edge from this node.
    '''

    def __init__(self, id):
        self.id = id
        self.prev = set()
