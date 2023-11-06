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

        # To work all code in bgsave
        # There must be various data type in redis db
        # so please before running this code, run another codes first
        res = self.r.bgsave()
        self.assertEqual(True, res)

    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()
