import hashlib
import bisect

class Node(object):
    """
    Server Node
    capacity would be treated as virtual nodes on ring
    """
    def __init__(self, hostname, port, capacity):
        self.hostname = hostname
        self.port = port
        self.capacity = capacity

    def __cmp__(self, rhs):
        return cmp((hostname, port), (rhs.hostname, rhs.port))

    def __repr__(self):
        """ return node info with 'hostname:port|cap' """
        return '%s:%d|%d' % (self.hostname, self.port, self.capacity)



class ConsistentHashRing(object):
    def __init__(self):
        self.hash = hashlib.sha1
        self.ring = dict()
        self._nodeKeys = []

    def addNode(self, node):
        """
        insert node onto ring with KEY="${nodeString}-${vnodeNumber}"
        """
        for i in range(0, node.capacity):
            pos = self.generatePosition("%s-%d" % (node, i))
            self.ring[pos] = node
            bisect.insort(self._nodeKeys, pos)
        
    def removeNode(self, node):
        """
        remove a node from ring
        """
        for i in range(0, node.capacity):
            pos = self.generatePosition("%s-%d" % (node, i))
            del self.ring[pos]
            self._nodeKeys.remove(pos)

    def resolveNode(self, key):
        """
        resolve node position from a key
        """
        if not self.ring:
           raise IndexError("Empty ring")

        pos = self.generatePosition(key)
        node = bisect.bisect_right(self._nodeKeys, pos)
        try:
            return self.ring[self._nodeKeys[node]]
        except IndexError:
            return self.ring[self._nodeKeys[0]]

    def generatePosition(self, key):
        """
        Get a string key and return a long integer as position in hash ring
        """
        return long(self.hash(key).hexdigest(), 16)
