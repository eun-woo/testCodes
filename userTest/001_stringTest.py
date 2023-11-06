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
        # set will replace any existing value already stored into the key
        res1 = self.r.set("string", "Test")
        self.assertEqual(res1, True)
        res2 = self.r.get("string")
        self.assertEqual(res2.decode(), "Test")

        # nx, xx option Test
        res3 = self.r.set("string", "experiment", nx=True)
        self.assertEqual(res3, None)
        self.assertEqual(self.r.get("string").decode(), "Test")

        res4 = self.r.set("string", "experiment", xx=True)
        self.assertEqual(res4, True)
        self.assertEqual(self.r.get("string").decode(), "experiment")

        # multipleSet, multipleGet Test
        res5 = self.r.mset({"clean:p": "prof.Tak", "clean:1": "Fatima", "clean:2": "Kim"})
        self.assertEqual(res5, True)
        res6 = self.r.mget(["clean:p", "clean:1", "clean:2"])
        res6 = list(map(decode, res6))
        self.assertEqual(res6, ["prof.Tak", "Fatima", "Kim"])

        # increrment
        self.r.set("total_increment", 0)
        res7 = self.r.incr("total_increment")
        # returns new value
        self.assertEqual(res7, 1)
        res8 = self.r.incrby("total_increment", 10)
        self.assertEqual(res8, 11)
        res9 = self.r.get("total_increment")
        # it's saved to binary string format
        self.assertEqual(self.r.get("total_increment").decode(), "11")
        self.r.set("float_increment", 0.5)

        # increment by float
        res10 = self.r.incrbyfloat("float_increment", 5)
        self.assertEqual(res10, 5.5)

        # for rdbSaveLzfStringObject
        self.r.set('key_string', 'abcdefghijklmnopqrstuvwxyz')


    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()
