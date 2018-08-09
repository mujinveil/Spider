#coding=utf-8

import io
import sys
from mitmproxy import ctx
import json
import pymongo
import time 

client=pymongo.MongoClient('localhost')
collection=db['douyin']
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')


def write_to_file(content):
	with open('douyin.txt','a',encoding='utf-8') as f:
		f.write(json.dumps(content,ensure_ascii=False)+'\n')



def response(flow):
     #print(flow.request.url)
     #print(flow.response.text)
     #global collection
     url1='https://aweme.snssdk.com/aweme/v1/music/aweme/?music_id'
     url2='https://api.amemv.com/aweme/v1/music/aweme/?music_id'
     if flow.request.url.startswith(url1) or flow.request.url.startswith(url2):
         text=flow.response.text

         data=json.loads(text)

         videos=data.get('aweme_list')
         print(videos[1])
         
         data={}
         for video in videos:
             timeStamp=video['create_time']

             timeArray = time.localtime(timeStamp)
             data['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

             #data['create_time']=video['create_time']
             data['author']=video['author'].get('nickname')
             data['author_id']=video['author_user_id']
             data['birthday']=video['author'].get('birthday')
             data['gender']=video['author'].get('gender')
             data['signature']=video['author'].get('signature')
             data['video_url']=video['share_info'].get('share_url')
             data['digg_count']=video['statistics'].get('digg_count')
             data['play_count']=video['statistics'].get('play_count')
             data['comment_count']=video['statistics'].get('comment_count')
             data['aweme_id']=video['statistics'].get('aweme_id')
             data['desc']=video['share_info'].get('share_weibo_desc').replace('#在抖音，记录美好生活#','')
             data['author']=video['author'].get('nickname')

             write_to_file(data)
             ctx.log.info(str(video))
             collection.insert(dict(data))
           