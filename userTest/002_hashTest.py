import unittest
import redis

# define to apply decode function to map function
def decode(s):
    assert type(s) is bytes
    return s.decode()

class RedisTest(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host='127.0.0.1', port=6379)

    def test_hash_set_and_get(self):
        # set will replace any existing value already stored into the key
        res1 = self.r.hset("car:1", mapping={
            "model": "Ionic",
            "brand": "Hyundai",
            "type": "Electronic car",
            "price": 4000,
            },
            )
        self.assertEqual(res1, 4)
        res2 = self.r.hget("car:1", "model")
        self.assertEqual(res2.decode(), "Ionic")

        # hash multi get
        res3 = self.r.hmget("car:1", ["model", "price"])
        self.assertEqual(list(map(decode, res3)), ["Ionic", "4000"])
        # hash field add
        res4 = self.r.hset("car:1", "tire", "on")
        res5 = self.r.hget("car:1", "tire")
        self.assertEqual(decode(res5), "on")
        #hash field get all
        hash_data = self.r.hgetall("car:1")
        print("hash_name: car:1")
        for key, value in hash_data.items():
            
            key_str = key.decode('utf-8')
            value_str = value.decode('utf-8')
            print(f"Key: {key_str}, Value: {value_str}")
        
        #hash field operation : return INT type 
        res8 = self.r.hincrby("car:1", "price", 100)
        self.assertEqual(res8, 4100)
    def tearDown(self):
        self.r.close()


if __name__=='__main__':
    unittest.main()
