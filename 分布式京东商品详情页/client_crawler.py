#coding=utf-8
from lxml import etree
import threading
import time
from mongo_redis_mgr import MongoRedisUrlManager
import argparse
import socket
from selenium import webdriver
import urllib3
import re
import os
import subprocess
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC 

# from hdfs import *
# from hdfs.util import HdfsError
from socket_client import SocketClient
import protocol_constants as pc
import json

import argparse
import hashlib



class arguments:
    pass

def parse_app_arguments():
    parser = argparse.ArgumentParser(prog='CrawlerClient', description='Start a crawler client')
    parser.add_argument('-S', '--host-all', type=str, nargs=1, help='Host server for all services')
    parser.add_argument('-s', '--host', type=str, nargs=1, help='Crawler host server address, default is localhost')
    parser.add_argument('-p', '--host-port', type=int, nargs=1, help='Crawler host server port number, default is 20100')
    parser.add_argument('-m', '--mongo', type=str, nargs=1, help='Mongo Server address, default is localhost')
    parser.add_argument('-n', '--mongo-port', type=int, nargs=1, help='Mongo port number, default is 27017')
    parser.add_argument('-r', '--redis', type=str, nargs=1, help='Redis server address, default is localhost')
    parser.add_argument('-x', '--redis-port', type=int, nargs=1, help='Redis port number, default is 6379')

    args = arguments()

    parser.parse_args(namespace=args)

    if args.host_all is not None:
        #args.host = args.mongo = args.redis = args.host_all
        args.host='localhost'

    if args.host is None:
        args.host = 'localhost'

    if args.mongo is None:
        args.mongo = 'localhost'

    if args.redis is None:
        args.redis = 'localhost'

    if args.host_port is None:
        args.host_port = 9999

    if args.mongo_port is None:
        args.mongo_port = 27017

    if args.redis_port is None:
        args.redis_port = 6379 

parse_app_arguments()


def get_page_content(cur_url, depth, driver):

    print ("downloading %s at level %d" % (cur_url, depth))

    wait=WebDriverWait(driver,30)
    number_tried = 0
    comment_num=''
    rate=''
    price=''
    item_name=''
    description=''
    #iid_match_obj = re_compiled_obj.search(cur_url)
    try:
        driver.get(cur_url)
        time.sleep(5)
        content = driver.page_source

        try:  
             item_name=wait.until(EC.presence_of_element_located((By.XPATH,"//DIV[@id='name']/H1[1]|//DIV[@class='sku-name']")))
             item_name=item_name.text
             price=wait.until(EC.presence_of_element_located((By.XPATH,'//div[@class="dd"]/strong[@class="p-price"]|//div[@class="dd"]/span[@class="p-price"]|//DIV[@class="summary-price J-summary-price"]/DIV[2]/SPAN[1]/SPAN[2]')))
             price=price.text
             description=wait.until(EC.presence_of_element_located((By.XPATH,"//DIV[@class='p-parameter']/UL[2]|//DIV[@class='p-parameter']"))) 

             description=description.text
             submit=wait.until(EC.element_to_be_clickable((By.XPATH,'//li[@data-offset="38"]')))
             submit.click()
             comment_num=submit.text.replace("(","").replace(")","").replace("商品评价","")
 
             rating=wait.until(EC.presence_of_element_located((By.XPATH,'//div[@class="percent-con"]')))
             rate=rating.text
        except :
             print('出错啦')
        print('++++++++++++')
        print(item_name)
        print(price)
        print(description)
        print(comment_num)
        print(rate)
        print('-----------')

        if len(re.findall('//item.jd.com/\d{7,11}.html', cur_url))>0:
            print('开始注入数据库')
            dbmanager.db.jd2.insert({
                '_id': hashlib.md5(cur_url.encode('utf8')).hexdigest(), 
                'url': cur_url, 
                'item_name': item_name, 
                'price': price, 
                'description':description,
                'comment_num':comment_num,
                'rate':rate
                 })
    

    except Exception as e:

        print ('出错啦',e)

        return



    dbmanager.finishUrl(cur_url)






    items = re.findall('//item.jd.com/\d{7,11}.html', content)

    #lists = re.findall('//list.jd.com/list.html\?cat=\d{3,5},\d{3,5},\d{3,5}', content)

    links = []



    for href in items: #+ lists:

        try:

            href = 'https:' + href

            links.append(href)

            dbmanager.enqueueUrl(href, 'new', depth+1)

        except ValueError:

            continue

    dbmanager.set_url_links(cur_url, links)


def get_web_driver():

    if len(webdrivers) == 0:

        for i in range(0, 1):
            driver=webdriver.Chrome()

            driver.set_window_size(1366,768) # optional

            webdrivers[driver] = False



    for dr in webdrivers:

        if webdrivers[dr] is False:

            return dr
def heartbeat():
    global server_status, run_heartbeat, client_id, hb_period
    skip_wait = False
    while run_heartbeat:
        if skip_wait is False:
            time.sleep(hb_period)
        else:
            skip_wait = False
        try:
            hb_request = {}
            hb_request[pc.MSG_TYPE] = pc.HEARTBEAT
            hb_request[pc.CLIENT_ID] = client_id
            print("sending a heartbeat! ", str(hb_request))
            hb_response_data = socket_client.send(json.dumps(hb_request))

            # should be network error
            if hb_response_data is None:
                continue
            
            # print( 'Heart Beat response', json.dumps(hb_response_data))
            response = json.loads(hb_response_data)

            err = response.get(pc.ERROR)
            if err is not None:
                if err == pc.ERR_NOT_FOUND:
                    register_request = {}
                    register_request[pc.MSG_TYPE] = pc.REGISTER
                    client_id = socket_client.send(json.dumps(register_request))

                    # skip heartbeat period and send next heartbeat immediately
                    skip_wait = True
                    heartbeat()
                    return
                return

            action = response.get(pc.ACTION_REQUIRED)
            if action is not None:
                action_request = {}
                if action == pc.PAUSE_REQUIRED:
                    server_status = pc.PAUSED
                    action_request[pc.MSG_TYPE] = pc.PAUSED
                elif action == pc.PAUSE_REQUIRED:
                    server_status = pc.RESUMED
                    action_request[pc.MSG_TYPE] = pc.RESUMED
                elif action == pc.SHUTDOWN_REQUIRED:
                    server_status = pc.SHUTDOWN
                    # stop heartbeat thread
                    return
                action_request[pc.CLIENT_ID] = client_id
                socket_client.send(json.dumps(action_request))
            else:
                server_status = response[pc.SERVER_STATUS]

        except socket.error as msg:
            print ("heartbeat error: ", msg)
            server_status = pc.STATUS_CONNECTION_LOST
            raise

def start_heart_beat_thread():
    try:
        t = threading.Thread(target=heartbeat, name=None)
        # set daemon so main thread can exit when receives ctrl-c
        t.setDaemon(True)
        t.start()
    except Exception as err:
        print( "Error: unable to start thread", err)
        raise

def crawl():
    # thread pool size
    max_num_thread = 5
    CRAWL_DELAY = 5
    global dbmanager, is_root_page, threads, hb_period 

    while True:
        if server_status == pc.STATUS_PAUSED:
            time.sleep(hb_period)
            continue
        if server_status == pc.SHUTDOWN:
            run_heartbeat = False
            for t in threads:
                t.join()
            break
        if server_status==pc.RESUMED:
            crawl()
        try:
            curtask = dbmanager.dequeueUrl()
        except Exception:
            time.sleep(hb_period)
            continue
        
        # Go on next level, before that, needs to wait all current level crawling done
        if curtask is None:
            time.sleep(hb_period)
            continue
        else:
            print( 'current task is: ', curtask['url'], "at depth: ", curtask['depth'])

        # looking for an empty thread from pool to crawl
        driver=get_web_driver()
        if is_root_page is True:
            #driver=get_web_driver()
            get_page_content(curtask['url'], curtask['depth'],driver)
            webdrivers[driver]=False
            is_root_page = False
        else:
            while True:    
                for t in threads:
                    if not t.is_alive():
                        threads.remove(t)
                if len(threads) >= max_num_thread:
                    time.sleep(CRAWL_DELAY)
                    continue
                try:
                    t = threading.Thread(target=get_page_content, name=None, args=(curtask['url'], curtask['depth'],driver))
                    threads.append(t)
                    time.sleep(CRAWL_DELAY)
                    # set daemon so main thread can exit when receives ctrl-c
                    t.setDaemon(True)
                    t.start()

                    break
                except Exception as err:
                    print( "Error: unable to start thread", err)
                    raise
def finish():
    global client_id
    print('关闭心跳')
    shutdown_request = {}
    shutdown_request[pc.MSG_TYPE] = pc.SHUTDOWN
    shutdown_request[pc.CLIENT_ID] = client_id
    socket_client.send(json.dumps(shutdown_request))


def init():
    global client_id
  

    dbmanager.clear()
    dbmanager.enqueueUrl('https://www.jd.com/allSort.aspx', 'new', 0 )

    register_request = {}
    register_request[pc.MSG_TYPE] = pc.REGISTER
    client_id = socket_client.send(json.dumps(register_request))




# custom header

headers = { 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',

        'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',

        'Accept-Charset':'utf-8',

        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',

        'Connection': 'keep-alive'

    }



# set custom headers

#for key, value in headers.items():

    #webdriver.DesiredCapabilities.PHANTOMJS['phantomjs.page.customHeaders.{}'.format(key)] = value



# another way to set custome header

#webdriver.DesiredCapabilities.PHANTOMJS['phantomjs.page.settings.userAgent'] ='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'

re_compiled_obj = re.compile('\d+')

constants = {

    'MAX_PAGE_TRIED': 2,

    'HB_PERIOD': 5,

    'MAX_SIZE_THREADPOOL': 5,

    'CRAWL_DELAY': 2

}
# Initialize system variables
#dir_name = 'mfw/'

# db manager
webdrivers={}
dbmanager = MongoRedisUrlManager()

is_root_page =True
threads = []
options = webdriver.ChromeOptions()
# 设置中文
options.add_argument('lang=zh_CN.UTF-8')
prefs = {"profile.managed_default_content_settings.images":2}
options.add_experimental_option("prefs",prefs)
# use hdfs to save pages
# hdfs_client = InsecureClient('http://54.223.92.169:50070', user='ec2-user')

socket_client = SocketClient('localhost', 20012)
client_id = 0

hb_period = 5
run_heartbeat = True
server_status = pc.STATUS_RUNNING

init()
start_heart_beat_thread()
crawl()
finish()

for dr in webdrivers:
    dr.close()
    dr.quit()