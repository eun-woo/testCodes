# hyperloglog is probablistic data structure that estimates the cardinality of a set
# hyperloglog trades perfect accuracy for efficient space utilization 
# uses up to 12kb, and probides a standard error of 0.81%

# estimate cardinality usually needs O(n) memory space (n is number of unique items)
# hyperloglog has standard error less than 1%
# up to 12kb, this need less memory when it has very few elements

# Serialize means changing format to use to another field
# Deserialize means resotring format

# Conceptually HLL API is like using Sets to do the same task.
# Every time you see a new element, you add it to the count with PFADD.
# Every time you want to retrieve the current approximation of the unique elements added with PFADD so far, you use the PFCOUNT


import unittest
import redis

class RedistTest(unittest.TestCase):
        def setUp(self):
            self.r = redis.Redis(host='localhost', port=6379)

        def test_bitmap(self):
                self.r.pfadd('hll', 'a', 'b', 'c', 'd')
                self.r.pfadd('hll', 'c', 'd', 'e', 'f')

                res = self.r.pfcount('hll')
                self.assertEqual(6, res)

                self.r.pfadd('hll2', 'e', 'f', 'f', 'g', 'h')

                self.r.pfmerge('hll', 'hll2')
                res = self.r.pfcount('hll')

                self.assertEqual(8, res)

        def tearDown(self):
                self.r.close()

if __name__ == '__main__':
        unittest.main()
