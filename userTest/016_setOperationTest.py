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
        # set initialization
        self.r.sadd('set_op_1', 'a', 'c')
        self.r.sadd('set_op_2', 'b', 'c')
        self.r.sadd('set_op_3', 'a', 'e')

        # sunion, sunionstore
        res = self.r.sunion('set_op_1', 'set_op_3')
        print(res)

        # 'a', 'b', 'c'
        self.r.sunionstore('set_op_4', 'set_op_1', 'set_op_2')

        # sdiff
        res = self.r.sdiff('set_op_3', 'set_op_4')

        # 'e'
        print(res)

        self.r.sdiffstore('set_op_5', 'set_op_3', 'set_op_4')

        # sscan
        print('sscan return value:', self.r.sscan('set_op_4',0))

    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()
