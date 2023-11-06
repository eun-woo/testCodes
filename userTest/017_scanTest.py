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
        cursor = self.r.scan(0)[0]
        while cursor:
            cursor = self.r.scan(cursor)[0]
        self.assertEqual(0, cursor)


    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()
