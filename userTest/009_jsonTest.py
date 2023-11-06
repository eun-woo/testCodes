import json
import redis

# 레디스 연결
rd = redis.StrictRedis(host='localhost', port=6379, db=0)

# dict 데이터 선언
dataDict = {
    "key1": "test1",
    "key2": "test2",
    "key3": "test3"
}

# json dumps
jsonDataDict = json.dumps(dataDict, ensure_ascii=False).encode('utf-8')

# 데이터 set
rd.set("dict", jsonDataDict)

# 데이터 get
resultData = rd.get("dict")
resultData = resultData.decode('utf-8')

# json loads
result = dict(json.loads(resultData))
print(result)
