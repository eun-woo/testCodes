# examples of bitmap use cases
# Efficient set representation for cases where the members of a set correspond to the integer 0-N
# Object permissions, where each bit represents a particular permission, similar to the way that file systems store permissions.

# SETBIT sets a bit at the 'provided offset' to 0 or 1
# GETBIT returns the value of a bit at a 'given offset'
# BIPOP lets you bitwise operations against one or more strings

# GETBIT returns 0 when index is out of range

# bit operations
# BITOP : AND, OR, XOR, NOT
# BITCOUNT : performs population counting, reporting the number of bits set to 1
# BITPOS : finds the 'first bit' having the specified value of 0 or 1

# setbit, getbit is O(1), bitop is O(N)

import unittest
import redis

class RedistTest(unittest.TestCase):
        def setUp(self):
                self.r = redis.Redis(host='localhost', port=6379)

        def test_bitmap(self):
                self.r.setbit('key', 0, 1)
                self.r.setbit('key', 10, 1)
                self.r.setbit('key', 10, 1)

                self.r.setbit('key2', 10, 1)
                self.r.bitop('AND', 'dest', 'key', 'key2')
                self.assertEqual(0 ,self.r.getbit('dest', 0))
                self.assertEqual(1, self.r.getbit('dest', 10))
		
        def tearDown(self):
                self.r.close()

if __name__ == '__main__':
	unittest.main()
