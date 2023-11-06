import unittest
import redis

# Convert a set with a byte string as an element 
# to a set with a string as an element
def decode_byte_set(byte_set):
    decoded_set = set()
    for item in byte_set:
        decoded_item = item.decode('utf-8')
        decoded_set.add(decoded_item)
    return decoded_set

class RedisTest(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host='127.0.0.1', port=6379)
        
    def test_set(self):
        # Adding elements to set
        self.r.sadd("myset", "apple")
        self.r.sadd("myset", "banana")
        self.r.sadd("myset", "cherry")
        

        # Remove elements from set
        self.r.srem("myset", "banana")

        # Check for elements in set
        self.assertEqual(decode_byte_set(self.r.smembers("myset")), {"apple", "cherry"})

        #Check whether element is in set
        is_apple_present = self.r.sismember("myset", "apple")
        is_banana_present = self.r.sismember("myset", "banana")
        
        self.assertEqual(is_apple_present, 1)
        self.assertEqual(is_banana_present, 0)
        
        # Using the SINTER command to calculate the intersection
        set1 = {"apple", "banana", "cherry"}
        set2 = {"banana", "cherry", "data"}
        set3 = {"cherry", "data", "fig"}

        self.r.sadd("set1", *set1)
        self.r.sadd("set2", *set2)
        self.r.sadd("set3", *set3)

        intersection = self.r.sinter("set1", "set2", "set3")
        self.assertEqual(decode_byte_set(intersection), {"cherry"})
    def tearDown(self):
        self.r.close()
if __name__=='__main__':
    unittest.main()
                     
