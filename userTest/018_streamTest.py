redis_host = "redis"
stream_key = "skey"
stream2_key = "s2key"
group1 = "grp1"
group2 = "grp2"

import redis
from time import time
from redis.exceptions import ConnectionError, DataError, NoScriptError, RedisError, ResponseError

r =  redis.StrictRedis(host='localhost', port=6379, db=0)
r.ping()

for i in range(0,10):
    r.xadd( stream_key, { 'ts': time(), 'v': i } )
print( f"stream length: {r.xlen( stream_key )}")

## read 2 entries from stream_key
l = r.xread( count=2, streams={stream_key:0} )
print(l)

first_stream = l[0]
print( f"got data from stream: {first_stream[0]}")
fs_data = first_stream[1]
for id, value in fs_data:
    print( f"id: {id} value: {value[b'v']}")

l = r.xread( count=2, streams={stream_key:0} )
for id, value in l[0][1]:
    print( f"id: {id} value: {value[b'v']}")

last_id_returned = l[0][1][-1][0]
l = r.xread( count=2, streams={stream_key: last_id_returned} )
for id, value in l[0][1]:
    print( f"id: {id} value: {value[b'v']}")

last_id_returned = l[0][1][-1][0]
l = r.xread( count=2, streams={stream_key: last_id_returned} )
for id, value in l[0][1]:
    print( f"id: {id} value: {value[b'v']}")


print( f"stream length: {r.xlen( stream_key )}")
# wait for 5s for new messages
l = r.xread( count=1, block=5000, streams={stream_key: '$'} )
print( f"after 5s block, got an empty list {l}, no *new* messages on the stream")
print( f"stream length: {r.xlen( stream_key )}")

for i in range(1000,1010):
    r.xadd( stream2_key, { 'v': i } )
print( f"stream length: {r.xlen( stream2_key )}")

l = r.xread( count=1, streams={stream_key:0,stream2_key:0} )
for k,d in l:
    print(f"got from {k} the entry {d}")



def add_some_data_to_stream( sname, key_range ):
    for i in key_range:
        r.xadd( sname, { 'ts': time(), 'v': i } )
    print( f"stream '{sname}' length: {r.xlen( stream_key )}")

add_some_data_to_stream( stream_key, range(0,10) )
add_some_data_to_stream( stream2_key, range(1000,1010) )

## create the group
def create_group( skey, gname ):
    try:
        r.xgroup_create( name=skey, groupname=gname, id=0 )
    except ResponseError as e:
        print(f"raised: {e}")

# group1 read the stream 'skey'
create_group( stream_key, group1 )
# group2 read the streams 'skey' and 's2key'
create_group( stream_key, group2 )
create_group( stream2_key, group2 )

def group_info( skey ):
    res = r.xinfo_groups( name=skey )
    for i in res:
        print( f"{skey} -> group name: {i['name']} with {i['consumers']} consumers and {i['last-delivered-id']}"
              + f" as last read id")

group_info( stream_key )
group_info( stream2_key )

def print_xreadgroup_reply( reply, group = None, run = None):
    for d_stream in reply:
        for element in d_stream[1]:
            print(  f"got element {element[0]}"
                  + f"from stream {d_stream[0]}" )
            if run is not None:
                run( d_stream[0], group, element[0] )

d = r.xreadgroup( groupname=group1, consumername='c', block=10,
                  count=2, streams={stream_key:'>'})
print_xreadgroup_reply( d )

# read some messages on group1 with consumer 'c'
d = r.xreadgroup( groupname=group1, consumername='c2', block=10,
                  count=2, streams={stream_key:'>'})
print_xreadgroup_reply( d )

d2 = r.xreadgroup( groupname=group2, consumername='c', block=10,
                   count=2, streams={stream_key:'>',stream2_key:'>'})
print_xreadgroup_reply( d2 )


# check pending status (read messages without a ack)
def print_pending_info( key_group ):
    for s,k in key_group:
        pr = r.xpending( name=s, groupname=k )
        print( f"{pr.get('pending')} pending messages on '{s}' for group '{k}'" )

print_pending_info( ((stream_key,group1),(stream_key,group2),(stream2_key,group2)) )

# do acknowledges for group1
toack = lambda k,g,e: r.xack( k,g, e )
print_xreadgroup_reply( d, group=group1, run=toack )

# check pending again
print_pending_info( ((stream_key,group1),(stream_key,group2),(stream2_key,group2)) )

d = r.xreadgroup( groupname=group1, consumername='c', block=10,
                      count=100, streams={stream_key:'>'})
print_xreadgroup_reply( d, group=group1, run=toack)
print_pending_info( ((stream_key,group1),) )

r.xlen(stream_key)

s1 = r.xread( streams={stream_key:0} )
for streams in s1:
    stream_name, messages = streams
    # del all ids from the message list
    [ r.xdel( stream_name, i[0] ) for i in messages ]


d2 = r.xreadgroup( groupname=group2, consumername='c', block=10,
                   count=2, streams={stream_key:'>',stream2_key:'>'})
print_xreadgroup_reply( d2 )
