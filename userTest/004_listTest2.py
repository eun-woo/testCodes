# List have a special feature that make them suitable to implement queues
# in general as a building block for inter process communication systems
# blocking option

# usual producer / consumer setup
# producer call LPUSH
# consumer call RPOP

# if the list is empty
# RPOP just returns NULL, this is called polling and it is not good idea
# When it receive NULL, it doesn't get usual work
# When you implement, when it receives NULL and wait a second.
# delay to processing of item, it waits a sometime. and during waiting time. there are redundant call of redis

# BRPOP, BLPOP which are version of RPOP, LPOP able to block
# when a new element is added to the list, or when a user-specified tikeout is reached, they'll return to the caller

import unittest
import redis

# defined to apply decode function to map function
def decode(s):
	assert type(s) is bytes
	return s.decode()

class RedisTest(unittest.TestCase):
	def setUp(self):
		self.r = redis.Redis(host='localhost', port=6379)

	def test_redis_list(self):
		self.r.rpush("bikes:repair", 'bike:1', 'bike:2')
		# 3rd parameter is timeout 1sec
		# 0 means wait forever
		# you can also specify muliple lists ans not just one
		# in order to wait on multiple lists at the same time, and get notified when the first list receives an element
		# the return value is different with rpop, because it waits multiple lists
		self.assertEqual(self.r.brpop('bikes:repair', 1)[1].decode(), 'bike:2')
		self.assertEqual(self.r.brpop('bikes:repair', 1)[1].decode(), 'bike:1')
		self.assertEqual(self.r.brpop('bikes:repair', 1), None)

		# When we add element to an aggregate data type. if the target key does not exist, an empty aggregate data type is created before adding the element.
		# When we remove elements from an aggregate data type, if the value remains empty, the key is automatically destroyed. (except Stream type)

		# Calling a LLEN, removing elements of empty key works well

		# List operation that manipulate elements within a list are usually O(n) ex. LINDEX, LINSERT and LSET, you can use Redis stream instead
	
	def tearDown(self):
		self.r.close()

if __name__ == '__main__':
	unittest.main()
