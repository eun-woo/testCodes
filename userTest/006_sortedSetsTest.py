# A Redis sorted set is a collection of unique strings ordered by an associated score.
# When more than one string has the same score, the strings are ordered lexicographically.
# Some use cases for sorted sets include: Leaderboards, Rate Limiter? (sliding window algorithm: study later)
# You can think of sorted set as a mix between a Set and Hash. Like sets, sorted sets are composed of unique, non-repeating string element.
# every element in a sorted set is associated with a floating point value, called the score. this is why the type is also similar to a hash.

import unittest
import redis

# defined to apply decode function to map function
def decode(s):
	assert type(s) is bytes
	return s.decode()

def tuple_decode(t):
	assert type(t) is tuple
	x = (t[0].decode(), t[1])
	return x

class RedisTest(unittest.TestCase):
	def setUp(self):
		self.r = redis.Redis(host='localhost', port=6379)

	def test_redis_list(self):
		# ZADD
		self.r.zadd('hackers', {'Alan Kay': 1940})
		self.r.zadd('hackers', {'Sophie Wilson': 1957})
		self.r.zadd('hackers', {'Richard Stallman': 1953})
		self.r.zadd('hackers', {'Anita Borg': 1949})
		self.r.zadd('hackers', {'Yukihiro Matsumoto': 1965})
		self.r.zadd('hackers', {'Hedy Lamarr': 1914})
		self.r.zadd('hackers', {'Claude Shannon': 1916})
		self.r.zadd('hackers', {'Linus Torvalds': 1969})
		self.r.zadd('hackers', {'Alan Turing': 1912})
		# zadd can use multiple score value pairs, even if this is not used in example above
		# zadd is implemented by skip list and hash, so zadd performs O(log N) operation
		arr = ['Alan Turing',
				'Hedy Lamarr',
				'Claude Shannon',
				'Alan Kay',
				'Anita Borg',
				'Richard Stallman',
				'Sophie Wilson',
				'Yukihiro Matsumoto',
				'Linus Torvalds']
		self.assertEqual(arr, list(map(decode, self.r.zrange('hackers', 0, -1))))
		# this can be used like min-heap
		# you can order them the opposite way by ZREVRANGE (z reverse range)
		self.assertEqual(list(reversed(arr)), list(map(decode, self.r.zrevrange('hackers', 0, -1))))
		# possible to return score as well, using the WITHSCORES argument
		brr = [('Alan Turing', 1912),
				('Hedy Lamarr',1914),
				('Claude Shannon',1916),
				('Alan Kay',1940),
				('Anita Borg',1949),
				('Richard Stallman',1953),
				('Sophie Wilson',1957),
				('Yukihiro Matsumoto',1965),
				('Linus Torvalds',1969)]
		x = self.r.zrange('hackers', 0, -1, withscores=True)
		self.assertEqual(brr, list(map(tuple_decode, x)))

		# zrangebyscore
		crr = ["Alan Turing", "Hedy Lamarr", "Claude Shannon", "Alan Kay", "Anita Borg"]
		# -inf, +inf can used to value
		self.assertEqual(crr, list(map(decode, self.r.zrangebyscore('hackers', '-inf', 1950))))

		# zremrangebyscore returns number of removed values
		self.assertEqual(4, self.r.zremrangebyscore('hackers', 1940, 1960))

		# zrank can ask rank of element, zrevrank command is also available in order to get the rank, considering the elements sorted a descnding way.
		self.assertEqual(3, self.r.zrank('hackers', 'Yukihiro Matsumoto'))

	def tearDown(self):
		self.r.close()

if __name__ == '__main__':
	unittest.main()
