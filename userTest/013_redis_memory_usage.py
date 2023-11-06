import redis

# Redis 서버에 연결
redis_host = 'localhost'  # Redis 서버 호스트 주소
redis_port = 6379  # Redis 포트 번호 (6000 포트 사용)
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
redis_client.flushdb()
# SET 명령어를 사용하여 key-value 저장
key = 'key'
value = 'value'
redis_client.set(key, value)

# MEMORY USAGE 명령어를 사용하여 메모리 사용량 확인
memory_usage = redis_client.memory_usage(key)
print(f'Memory usage for key: {memory_usage} bytes')

# 다른 key-value 쌍 저장
key1 = 'key1'
value1 = 'AAAAAAAAAABBBBBBBBBB'
redis_client.set(key1, value1)

# MEMORY USAGE 명령어를 사용하여 key1의 메모리 사용량 확인
memory_usage_key1 = redis_client.memory_usage(key1)
print(f'Memory usage for key1: {memory_usage_key1} bytes')

# 루프를 사용하여 mylist에 데이터 추가
for i in range(1000):
    value = f'AAAAAAAAAA{i}'
    redis_client.lpush('mylist', value)

# MEMORY USAGE 명령어를 사용하여 mylist의 메모리 사용량 확인
memory_usage_mylist = redis_client.memory_usage('mylist', samples=0)
print(f'Memory usage for mylist: {memory_usage_mylist} bytes')

# 루프를 사용하여 myset에 데이터 추가
for i in range(1000):
    value = f'AAAAAAAAAA{i}'
    redis_client.sadd('myset', value)

# MEMORY USAGE 명령어를 사용하여 myset의 메모리 사용량 확인
memory_usage_myset = redis_client.memory_usage('myset', samples=0)
print(f'Memory usage for myset: {memory_usage_myset} bytes')

# 루프를 사용하여 myzset에 데이터 추가
for i in range(1000):
    value = f'AAAAAAAAAA{i}'
    score = i
    redis_client.zadd('myzset', {value: score})

# MEMORY USAGE 명령어를 사용하여 myzset의 메모리 사용량 확인
memory_usage_myzset = redis_client.memory_usage('myzset', samples=0)
print(f'Memory usage for myzset: {memory_usage_myzset} bytes')

# 루프를 사용하여 myhash에 데이터 추가
for i in range(1000):
    field = f'field{i}'
    value = f'AAAAAAAAAA{i}'
    redis_client.hset('myhash', field, value)

# MEMORY USAGE 명령어를 사용하여 myhash의 메모리 사용량 확인
memory_usage_myhash = redis_client.memory_usage('myhash', samples=0)
print(f'Memory usage for myhash: {memory_usage_myhash} bytes')
