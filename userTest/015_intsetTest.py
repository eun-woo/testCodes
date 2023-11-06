import redis

r = redis.Redis(host='localhost', port=6379)

r.sadd('intset', '0')
r.sadd('intset', '2', '4')
r.sadd('intset', '6')


# 0, 2, 4, 6
res = r.smembers('intset')
print(res)

# 0, 2, 3, 4, 6
r.sadd('intset', '3')

res = r.smembers('intset')

print(res)

# remove existing value and non-existing value
r.srem('intset', '6')
r.srem('intset', '5')

# 0, 2, 3, 4
print(r.smembers('intset'))

# random remove
r.spop('intset')

# must return 3
print(r.scard('intset'))

# one member nonexist , three member exist
r.sismember('intset', '0')
r.sismember('intset', '2')
r.sismember('intset', '3')
r.sismember('intset', '4')

# to call intsetRandom
r.srandmember('intset')

# there are members in set
r.spop('intset')
r.spop('intset')
r.spop('intset')

# there are no member in set
r.spop('intset')

r.close()
