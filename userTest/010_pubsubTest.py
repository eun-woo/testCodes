import unittest
import redis

class RedisTest(unittest.TestCase):
    def setUp(self):
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe('my-channel')

    def test_publish_and_subscribe_to_a_channel(self):
        message = "Hello, World!"

        # channel is not present now. can this code make(or open)

        self.redis_client.publish('my-channel', message)
        received_message = self.pubsub.get_message()
        received_message = self.pubsub.get_message()

        # why received messgae saved to key 'data'

        self.assertEqual(received_message['data'].decode(), message)

    def tearDown(self):
        self.pubsub.unsubscribe('my-channel')
        self.redis_client.close()


if __name__=='__main__':
    unittest.main()
