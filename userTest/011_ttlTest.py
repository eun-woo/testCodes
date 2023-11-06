import redis
import time

r = redis.Redis(host='localhost', port=6379)
# set keys
r.set('key_expire', 'value1')

# set TTL to key_1
r.expire('key_expire', 2)

res = r.ttl('key_expire')

# it must be 2
print(res)

time.sleep(3)
res = r.ttl('key_expire')

# it must be -2
print(res)

r.close()
