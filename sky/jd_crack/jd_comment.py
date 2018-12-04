#!/usr/bin/env python3
# coding=utf-8
import json
import logger
import pymongo
import requests
from lxml import etree
from gevent.pool import Pool
import gevent.monkey
import time
from redis import StrictRedis, ConnectionPool
import pymongo

client = pymongo.MongoClient(host='192.168.31.142', port=27017)
db = client['spider']
collection = db['item_detail']

connection_pool = ConnectionPool(host='192.168.31.142', port=6379, db=8)
redis_conn = StrictRedis(connection_pool=connection_pool)


class JDCommentSpider(object):

    __slots__ = ['comment_url']

    def __init__(self):
        self.comment_url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON' \
                           '_comment98vv118416&productId={0}&score=0&sortType=5&page={1}&pageSize=10'

    def get_sku_ids(self):
        docs = collection.find()
        sku_ids = []
        good_names = []
        for doc in docs:
            sku_id = doc.get('sku_id', None)
            good_name = doc.get('name', None)
            if good_name:
                good_name = good_name.split(" ")[0]
            else:
                good_name = None

            sku_ids.append(sku_id)
            good_names.append(good_name)

        items = set(list(zip(sku_ids, good_names)))
        items = list(items)
        return items

    def start(self):
        items = self.get_sku_ids()
        for sku_id, good_name in items:
            url = self.comment_url.format(sku_id, 1)
            data = {'callback': 'parse_comment', 'url': url, 'sku_id': sku_id, 'page': 1,
                    'good_name': good_name}
            redis_conn.rpush('jd_comment', json.dumps(data))

    def parse_comment(self, data):
        url = data.get('url')
        sku_id = data.get('sku_id')
        page = data.get('page')
        good_name = data.get('good_name')
        content = requests.get(url).text
        comment_item = {}
        if content != '':

            content = content.replace(r"fetchJSON_comment98vv118416(", "")
            content = content.replace(r");", "")
            data = json.loads(content)
            max_page = data.get("maxPage")
            comments = data.get("comments")
            comment_item['sku_id'] = sku_id
            comment_item['good_name'] = good_name
            comment_item['item_name'] = 'comment'

            for comment in comments:

                comment_item['comment_id'] = comment.get('id')  # 评论的 id
                comment_item['content'] = comment.get('content')  # 评论的内容
                comment_item['creation_time'] = comment.get('creationTime', '')  # 评论创建的时间
                comment_item['reply_count'] = comment.get('replyCount', '')  # 回复数量
                comment_item['score'] = comment.get('score', '')  # 评星
                comment_item['useful_vote_count'] = comment.get('usefulVoteCount',
                                                                '')  # 其他用户觉得有用的数量
                comment_item['useless_vote_count'] = comment.get('uselessVoteCount',
                                                                 '')  # 其他用户觉得无用的数量
                comment_item['user_level_id'] = comment.get('userLevelId', '')  # 评论用户等级的 id
                comment_item['user_province'] = comment.get('userProvince', '')  # 用户的省份
                comment_item['nickname'] = comment.get('nickname', '')  # 评论用户的昵称
                comment_item['user_level_name'] = comment.get('userLevelName', '')  # 评论用户的等级
                comment_item['user_client'] = comment.get('userClient', '')  # 用户评价平台
                comment_item['user_client_show'] = comment.get('userClientShow', '')  # 用户评价平台
                comment_item['is_mobile'] = comment.get('isMobile', '')  # 是否是在移动端完成的评价
                comment_item['days'] = comment.get('days', '')  # 购买后评论的天数
                comment_item['reference_time'] = comment.get('referenceTime', '')  # 购买的时间
                comment_item['after_days'] = comment.get('afterDays', '')  # 购买后再次评论的天数
                after_user_comment = comment.get('afterUserComment', '')

                if after_user_comment != '' and after_user_comment is not None:
                    h_after_user_comment = after_user_comment.get('hAfterUserComment', '')
                    after_content = h_after_user_comment.get('content', '')  # 再次评论的内容
                    comment_item['after_user_comment'] = after_content

                db['jd_comment'].insert(comment_item)

        if page < max_page:
            data['page'] = page + 1
            data['url'] = self.comment_url.format(sku_id, data['page'])
            redis_conn.rpush('jd_comment', json.dumps(data))

        if page >= max_page:
            summary_item = {}
            comment_summary = data.get("productCommentSummary")
            summary_item['item_name'] = 'comment_summary'
            summary_item['poor_rate'] = comment_summary.get('poorRate')
            summary_item['good_rate'] = comment_summary.get('goodRate')
            summary_item['good_count'] = comment_summary.get('goodCount')
            summary_item['general_count'] = comment_summary.get('generalCount')
            summary_item['poor_count'] = comment_summary.get('poorCount')
            summary_item['after_count'] = comment_summary.get('afterCount')
            summary_item['average_score'] = comment_summary.get('averageScore')
            summary_item['sku_id'] = sku_id
            summary_item['good_name'] = good_name
            db['jd_comment_summary'].insert(summary_item)

    def run(self):
        while True:
            try:
                data = redis_conn.lpop('jd_comment')
                if len(data) == 0:
                    return
            except Exception as e:
                pass

            else:
                data = json.loads(data)
                getattr(self, data.get('callback'))(data)


if __name__ == "__main__":
    gevent.monkey.patch_all()
    crawler = JDCommentSpider()
    p = Pool(10)
    #crawler.start()
    time.sleep(2)

    for i in range(3):
        p.apply_async(crawler.run)
    p.join()

