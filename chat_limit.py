import os
import sys

import json
import struct
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0,'/home/zheng.cheng/')
from pyDes import *
import pyspark.sql.functions as func
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, ShortType
from pyspark.sql.window import Window
from contrib.dataregistry import *

DATETIME_FORMAT_PARSER = "%Y%m%d-%H%M%S"
DATETIME_FORMAT_PRINTER = "%Y-%m-%d %H:%M:%S.%f"

def parse_uid(user_id):
    raw = struct.pack("!Q", user_id)
    key = "\x01\x09\x09\x00\x00\x07\x01\x03"
    k = des("DESCRYPT", CBC, key, pad=None, padmode=None)
    d = k.decrypt(raw)
    r = struct.unpack("!Q", d)[0]
    return r

def parse_message_type(key):
    return {
        "CUSTOM_MSG_TYPE_TEXT": 'text',
        "CUSTOM_MSG_TYPE_OFFLINE_VOICE": 'voice',
        "CUSTOM_MSG_TYPE_IMAGE": 'image',
        "push_emoticon": 'emoticon',
        "CUSTOM_MSG_TYPE_VIDEO": 'video',
    }.get(key.strip(), 'others')

def parse_uniqueid_type(key):
    return {
        32: 'other_voice',
    }.get(key, 'others')

def parse_message_content(raw_message):
    # let exception raise here
    message = json.loads(raw_message)
    try:
        type_ = parse_message_type(message.get('type'))
    except AttributeError:
        print("can not parse use old type")
        type_ = parse_uniqueid_type(message.get('messageType'))
    ret = {
        'msg_type': type_,
        'msg_id': message.get('messageId') if type_ != 'other_voice' else  message.get('uniqueId') ,
        'content': None,
        'duration': None
    }
    if type_ == 'text':
        ret['content'] = message.get('message', {}).get('text')
    elif type_ == 'voice':
        ret.update({'content': message.get('download-url'),'duration': int(message.get('duration', '0'))})
    elif type_ == 'image':
        ret['content'] = message.get('message', {}).get('image_url')
    elif type_ == 'emoticon':
        ret['content'] = message.get('message', {}).get('emoticon_title')
    elif type_ == 'video':
        ret.update({ 'content': message.get('message', {}).get('video_url'),'duration': int(message.get('message', {}).get('duration', '0'))})
    elif type_ == 'other_voice':
        msg = message.get('messageContent')
        msgBody = json.loads(msg)
        ret.update({'content': msgBody.get('download-url'), 'duration': int(msgBody.get('duration', '0'))})
    return ret

def parse_call_flow(line):
    #line = line.encode('utf-8').strip()
    i = line.find("{")
    j = line.rfind("}")
    info = line[:i]
    original_message = line[i:j+1]
    split_items = line[:i].strip().split('|')
    print(split_items)
    unformat_time = split_items[0]
    sender = split_items[1]
    print(info)
    if sender == u'server':
        print("sender ==sever")
        raise ValueError('invalid record: %s' % line)
    else:
        message_info = parse_message_content(original_message)
        receiver = split_items[2]
        dt = datetime.datetime.strptime(unformat_time, DATETIME_FORMAT_PARSER)
        send_time = dt.strftime(DATETIME_FORMAT_PRINTER)
        group_id = split_items[1] +'_'+ split_items[2] if  split_items[1] > split_items[2] else split_items[2] +'_'+ split_items[1]
        result =  {
            'group_id': group_id,
            'sender':  parse_uid(int(sender)),
            'receiver': parse_uid(int(receiver)),
            'send_time':send_time,
        }
        result.update(message_info)
        return result

def process(parser, line):
   #return [parser(line), ]
    try:
        return [parser(line), ]
    except:
        print("exception!!!!!!!!!!")
        return []


def run(date) :
    spark = SparkSession.builder \
        .appName("DialerChatSink") \
        .config("spark.hadoop.dfs.replication", 1) \
        .getOrCreate()
    sc = spark.sparkContext
    lines = sc.textFile('/user/drill/almark/chat_limit/{}/'.format(date))
    records = lines\
        .flatMap(lambda line: process(parse_call_flow, line)) \
        .map(lambda r: Row(
            group_id=r['group_id'],
            msg_id=r['msg_id'],
            msg_type=r['msg_type'],
            sender=r['sender'],
            receiver=r['receiver'],
            send_time=r['send_time'],
            content=r['content'],
            duration=r['duration'],
            )
        )
    schema = StructType([
        StructField('group_id', StringType(), True),
        StructField('msg_id', StringType(), True),
        StructField('msg_type', StringType(), True),
        StructField('sender', LongType(), True),
        StructField('receiver', LongType(), True),
        StructField('send_time', StringType(), True),
        StructField('content', StringType(), True),
        StructField('duration', LongType(), True)
    ])
    frame = spark.createDataFrame(records, schema)
    frame.write.mode("overwrite").parquet("/user/drill/zhengcheng/temp_data_chat_{}/".format(date))

def update():
    d = datetime.datetime.today()

if __name__ == '__main__':
# date = '20170713'
#   run_with_date(date)
#   date = '20170714'
#   run_with_date(date)
    date = '20170715'
    run(date)
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        date_string = sys.argv[1]
        date_ = datetime.datetime.strptime(date_string, '%Y%m%d')
        run(date_)
    else:
        update()
