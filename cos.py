import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from config import *
import pymongo
import re
import os
from hashlib import md5
from multiprocessing import Pool
client = pymongo.MongoClient(HOST,PORT)
db = client[DATABASE]
col = db[KEYWORD]

def parse_page_url(url):
    '''
    爬取ajax请求的某个url
    :param url: ajax发送请求的url
    :return: 包含当前页面所有item数据的python字典
    '''
    try:                                 # 加一个异常处理。
        r = requests.get(url,headers=HEADERS)
        if r.status_code == 200:
            return r.json()               # requests模块提供了json(),直接将json数据load后返回。
    except RequestException:
        print('{} 网页不能访问!!'.format(url))
        return

def get_detail_url(dic):
    '''
    获取每个item具体的信息，以字典格式返回
    :param dic: 包含某个页面所有item的一个大字典
    :return: 包含某个item的详细信息的小字典
    '''
    if dic and 'data' in dic:
        for item in dic['data']:
            if 'media_creator_id' in item.keys():
                Data = {
                    'url':item['url'],
                    'title':item['title']
                }
                yield Data

def get_detail_info(dic):
    '''
    获取每个item页面的所有图片的url。
    :param dic: 包含某个item的详细信息的字典
    :return: 返回item页面所有图片的url组成的列表
    '''
    detail_url = dic['url']
    title = dic['title']
    try:
        r = requests.get(detail_url, headers=HEADERS)
    except RequestException:
        return
    # item页面设计的比较好，页面内的图片的url，用BeautifulSoup取不出来，隐藏在doc-->Resoponse的一个字典中，这里用正则匹配查找出来
    pics = [i.split('&')[0] for i in re.findall('http://p3.*?&', r.text)]
    # 过滤重复的url
    return list(set(pics))
def encrypt(content):
    '''
    对图片的二进制数据，进行md5哈希，防止重复下载
    :return:
    '''
    m = md5()
    m.update(content)
    return m.hexdigest()
def download(pics):
    '''
    将图片下载，保存在KEYWORD命名的文件夹内。
    :param pics:
    :return:
    '''
    if pics:
        for pic in pics:
            try:
                r = requests.get(pic,headers=HEADERS)
            except RequestException:
                return
            if r.content:
                if not os.path.exists(r'{}'.format(KEYWORD)):
                    os.mkdir(r'{}'.format(KEYWORD))
                # filename = r'{}\{}.{}'.format(KEYWORD,encrypt(r.content),'jpg') if os.path.exists(r'{}'.format(KEYWORD))
                filename = r'{}\{}.{}'.format(KEYWORD,encrypt(r.content),'jpg')
                # 判断图片是否下载过
                if os.path.exists(filename):
                    continue
                f = open(filename,'wb')
                f.write(r.content)
                f.close()
                print('{} 文件下载完成'.format(filename))

def save_to_mongo(pics):
    '''
    保存每张图片的url到数据库中。
    :param pics:
    :return:
    '''
    if pics:
        for pic in pics:
            if not col.find({'url':pic}).count():
                col.insert_one({'url':pic})

def main(url):
    page_dic = parse_page_url(url)
    for item_dic in  get_detail_url(page_dic):
        pics = get_detail_info(item_dic)
        download(pics)
        save_to_mongo(pics)



if __name__ == '__main__':
    p = Pool()
    p.map(main,['https://www.toutiao.com/search_content/?offset={}&format=json&keyword={}&autoload=true&count=20&cur_tab=1&from=search_tab'.format(i*20,KEYWORD) for i in range(STARTNUM,STOPNUM)])
    p.close()
    p.join()
