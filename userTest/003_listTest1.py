# redis list is lists of string values
# LPUSH adds a new element to the head of a list; RPUSH adds to the tail
# LPOP removes and returns an element from the head of a list; RPOP does the same but from the tails of a list
# LLEN returns the length of a list
# LMOVE atomically moves elements from one list to another
# LTRIM reduces a list to the specified range of elements

# Blocking commands

# Lists support several blocking commands. For example
# BLPOP removes and returns an element from the head of a list. If the list is empty, the command blocks until an element becomes available or until the specified timeout is reached.
# BLMOVE atomically moves elements from a source list to a target list. If the source list is empty, the command will block until a new element becomes available.


import unittest
import redis

# define to apply decode function to map function
def decode(s):
    assert type(s) is bytes
    return s.decode()


class RedisTest(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host='127.0.0.1', port=6379)

    def test_publish_and_subscribe_to_a_channel(self):
        # list initialization
        init_len = self.r.llen("bikes:repairs")
        for i in range(init_len):
            self.r.lpop("bikes:repairs")
        init_len2 = self.r.llen("bikes:finished")
        for i in range(init_len2):
            self.r.lpop("bikes:finished")

        # treat a list like a queue
        self.r.lpush('bikes:repairs', 'bike:1')
        self.r.lpush('bikes:repairs', 'bike:2')
        self.assertEqual('bike:1', self.r.rpop('bikes:repairs').decode())
        self.assertEqual('bike:2', self.r.rpop('bikes:repairs').decode())

        # treat a list like a stack and check the length of a list
        # llen returns a integer
        self.r.rpush('bikes:repairs', 'bike:1')
        self.r.rpush('bikes:repairs', 'bike:2')

        self.assertEqual(2, self.r.llen('bikes:repairs'))

        self.assertEqual('bike:2', self.r.rpop('bikes:repairs').decode())
        self.assertEqual(1, self.r.llen('bikes:repairs'))

        self.assertEqual('bike:1', self.r.rpop('bikes:repairs').decode())
        self.assertEqual(0, self.r.llen('bikes:repairs'))

        # Atomically pop an element from one list and push to another
        for i in range(1, 11):
            self.r.lpush('bikes:repairs', 'bike:' + str(i))
        for i in range(1, 11):
            self.r.lmove('bikes:repairs', 'bikes:finished', 'right', 'left')
        for i in range(1, 11):
            self.assertEqual('bike:' + str(i), self.r.rpop('bikes:finished').decode())

        # LRANGE
        arr = []
        for i in range(1, 11):
            self.r.lpush('bikes:repairs', 'bike:' + str(i))
            arr.append('bike:' + str(i))
        arr.reverse()
        brr = list(map(decode, self.r.lrange('bikes:repairs', 0, -1)))
        self.assertEqual(brr, arr)

        #LTRIM, LREM, LPUSHX
        self.r.ltrim('bikes:repairs', 0, 8)
        # remove existing value
        res = self.r.lrem('bikes:repairs', 1, 'bike:1')
        # remove non existing value
        self.r.lrem('bieks:repairs', 2, 'nonexist')
        # lpushx if value exists
        self.r.lpushx('bikes:repairs', 'bike:100')
        # nonexists
        self.r.lpushx('supercars', 'super super car')
		

    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()


# Redis returned a NULL value to signal that there are no elements in the list
# Redis List is Linked List! It's very important
# LRANGE is used to look up the newest informations
# LTRIM is used to limit size of list
# LRANGE is technically O(N) commands, but accessing small ranges toward the head or the tail of the list is a constant time operation


