import redis

# Redis 서버에 연결
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# 파이프라인 시작
pipe = r.pipeline()

# 여러 Redis 명령을 파이프라인에 추가
pipe.set('key1', 'value1')
pipe.set('key2', 'value2')
pipe.get('key1')
pipe.get('key2')

# 모든 명령을 일괄로 실행하고 결과를 받음
results = pipe.execute()

# 결과 출력
for result in results:
    print(result)
