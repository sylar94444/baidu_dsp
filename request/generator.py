# -*- coding: utf-8 -*-
#!/usr/bin/env python
#Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved 

import random
import time
import string

import baidu_realtime_bidding_pb2

MAX_MINIMUM_CPM = 10000
MAX_AD_BLOCK_KEY=1000*100
ID_LENGTH = 32
BAIDU_USER_ID_LENGTH = 32
BAIDU_USER_ID_VERSION = 1
MAX_CREATIVE_TYPE_NUM = 4
MAX_PAGE_KEYWORD_NUM = 3
MAX_EXCLUDED_PRODUCT_CATEGORY_NUM = 10
MAX_EXCLUDED_LANDING_PAGE_URL_NUM = 10

#页面语言
LANGUAGE_CODES = ['en',"zh-cn","zh-tw" ]
#用户性别
GENDER = [ 0, 1, 2]
#允许的创意类型
CREATIVE_TYPES = [ 1, 2 ]
#广告位位置信息
SLOT_VISIBILITY = [ 0, 1, 2 ]
#展示类型
ADSLOT_TYPE = [ 0, 1, 2 ] 
#站点分类
SITE_CATEGORY = [ i for i in range(1,24) ] 
#网站质量类型
SITE_QUALITY = [ 0, 1, 2, 3 ] 
#页面类型
PAGE_TYPE = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 ] 
#页面内容质量
PAGE_QUALITY = [ 0, 1 ] 
#广告行业
PRODUCT_CATEGORY = [ i for i in range(54,87) ]
#页面关键词
PAGE_KEYWORDS = [ "baidu1", 
                  "baidu2",
                  "baidu3",
                  "baidu4",
                  "baidu5",
                  "baidu6",
                  "baidu7",
                  "baidu8",
                  "baidu9",
                  "baidu10",
]
#landing page 列表
LANDING_PAGE_URLS = [
        "www.landing_page1.com",
        "www.landing_page2.com",
        "www.landing_page3.com",
        "www.landing_page4.com",
        "www.landing_page5.com",
        "www.landing_page6.com",
        "www.landing_page7.com",
        "www.landing_page8.com",
        "www.landing_page9.com",
        "www.landing_page10.com",
]
#广告位所在的url信息
URLS = [
        "www.baidu1.com",
        "www.baidu2.com",
        "www.baidu3.com",
        "www.baidu4.com",
        "www.baidu5.com",
        "www.baidu6.com",
        "www.baidu7.com",
        "www.baidu8.com",
        "www.baidu9.com",
        "www.baidu10.com",
]
# referer ulr信息
REFERERS = [
        "www.baidu_refer1.com",
        "www.baidu_refer2.com",
        "www.baidu_refer3.com",
        "www.baidu_refer4.com",
        "www.baidu_refer5.com",
        "www.baidu_refer6.com",
        "www.baidu_refer7.com",
        "www.baidu_refer8.com",
        "www.baidu_refer9.com",
        "www.baidu_refer10.com",
]
#广告位的宽高,像素
AD_SIZES = [
        (300, 250),
        (960, 90),
        (336, 280),
        (200, 200),
        (728, 90),
        (640, 60),
        (640, 80),
        (120, 600),
        (250, 250),
        (960, 60),
        (468, 60),
        (160, 600),
]
#用户兴趣信息
USER_CATEGORY = [ 
        240010,
        240020,
        240030,
        240040,
        240050,
        240060,
        240070,
        240080,
        240090,
        250010,
        250020,
        270010,
]
MAX_USER_CATEGORY = 4
#用户user agent 信息
USER_AGENTS = [
    'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.2) '
    'Gecko/2008092313 Ubuntu/8.04 (hardy) Firefox/3.1',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) '
    'Gecko/20070118 Firefox/2.0.0.2pre',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.7pre) Gecko/20070815 '
    'Firefox/2.0.0.6 Navigator/9.0b3',
    'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_4_11; en) AppleWebKit/528.5+'
    ' (KHTML, like Gecko) Version/4.0 Safari/528.1',
    'Mozilla/5.0 (Macintosh; U; PPC Mac OS X; sv-se) AppleWebKit/419 '
    '(KHTML, like Gecko) Safari/419.3',
    'Mozilla/5.0 (Windows; U; MSIE 7.0; Windows NT 6.0; en-US)',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0;)',
    'Mozilla/4.08 (compatible; MSIE 6.0; Windows NT 5.1)',
]

#移动设备信息
#(device_type, platform, os major version, os minor version, os microversion
#is_app, brand,model, screen_width,screen_height, user_agent,
#wireless_network_type, CARRIER_ID)
UNKNOWN_DEVICE = baidu_realtime_bidding_pb2.BidRequest.Mobile.UNKNOWN_DEVICE
HIGHEND_PHONE = baidu_realtime_bidding_pb2.BidRequest.Mobile.HIGHEND_PHONE
TABLET = baidu_realtime_bidding_pb2.BidRequest.Mobile.TABLET
#设备平台
UNKNOWN_OS = baidu_realtime_bidding_pb2.BidRequest.Mobile.UNKNOWN_OS 
IOS = baidu_realtime_bidding_pb2.BidRequest.Mobile.IOS
ANDROID = baidu_realtime_bidding_pb2.BidRequest.Mobile.ANDROID
WINDOWS_PHONE = baidu_realtime_bidding_pb2.BidRequest.Mobile.WINDOWS_PHONE
#无线设备
WIFI = baidu_realtime_bidding_pb2.BidRequest.Mobile.WIFI
MOBILE_3G = baidu_realtime_bidding_pb2.BidRequest.Mobile.MOBILE_3G
MOBILE_4G = baidu_realtime_bidding_pb2.BidRequest.Mobile.MOBILE_4G

MOBILE_DEVICE_INFO = [
    (HIGHEND_PHONE, IOS, 6, 1, 2, True, 'apple',"iPhone5,2", 320, 50,
     'Mozilla/5.0 (iPhone; CPU iPhone OS 6_1_2 like Mac OS X) AppleWebKit/'
     '536.26 (KHTML, like Gecko) Mobile/10B146,gzip(gfe)', MOBILE_3G, 46001),
    (HIGHEND_PHONE, ANDROID, 2, 3, 6, True, 'htc', 'g7', 320, 50,
     'Mozilla/5.0 (Linux; U; Android 2.3.6; it-it; GT-S5570I Build/GINGERBREAD)'
     ' AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1 '
     '(Mobile; afma-sdk-a-v6.1.0),gzip(gfe)', MOBILE_4G, 46000),
    (TABLET, ANDROID, 4, 1, 1, True, 'samsung', 'GalaxyN8000', 728, 90,
     'Mozilla/5.0 (Linux; U; Android 4.1.1; fr-ca; GT-P3113 Build/JRO03C) '
     'AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30 (Mobile;'
     ' afma-sdk-a-v6.1.0),gzip(gfe)', MOBILE_3G, 46001),
    (TABLET, IOS, 6, 1, 2, True, 'apple', 'ipad3', 768, 1024,
     'Mozilla/5.0 (iPad; CPU OS 6_1_2 like Mac OS X) AppleWebKit/536.26'
     ' (KHTML, like Gecko) Mobile/10B146,gzip(gfe)', WIFI, 46000),
    (UNKNOWN_DEVICE, UNKNOWN_OS, 9, 2, 20, False, 'blackberry', '9220', 320, 50,
     'Mozilla/5.0 (BlackBerry; U; BlackBerry 9220; en) AppleWebKit/534.11+ '
     '(KHTML, like Gecko) Version/7.1.0.337 Mobile Safari/534.11+,gzip(gfe)', WIFI, 46001)
]

DEFAULT_MOBILE_PROPORTION = 0.2
#用户位置信息
#省份，城市，地区，街道
USER_LOCATION_INFO = [
    ("北京", "北京市", "上地", "十街10号"),
    ("上海", "上海市", "张江区", "百度研发中心")
]
#经纬度坐标信息 latitude,longitude,standard
BD_09 = baidu_realtime_bidding_pb2.BidRequest.Geo.Coordinate.BD_09
GCJ_02 = baidu_realtime_bidding_pb2.BidRequest.Geo.Coordinate.GCJ_02
WGS_84 = baidu_realtime_bidding_pb2.BidRequest.Geo.Coordinate.WGS_84
COORDINATE_INFO = [
    ( -89.87, -140.34, BD_09),
    ( 41.06, 115.25, GCJ_02),
    ( 39.54, 116.23, WGS_84)
]

#app交互信息 app_interaction_type define
TELEPHONE = baidu_realtime_bidding_pb2.BidRequest.Mobile.MobileApp.TELEPHONE
DOWNLOAD = baidu_realtime_bidding_pb2.BidRequest.Mobile.MobileApp.DOWNLOAD
#APP信息 app_id, app_bundle_id, app_category, app_interaction_type
MOBILE_APP_INFO = [
        ('a90ad199', 'test_app_id123', 9901, TELEPHONE),
        ('b30ad187', 'test_app_id234', 5203, DOWNLOAD)
]
random.seed(time.time())

class BidGeneratorManager(object):
    """
    @note:按照比例随机生成移动/pc请求
    @return
    """
    def __init__(self, mobile_proportion=DEFAULT_MOBILE_PROPORTION):
        self._mobile_proportion = mobile_proportion
        self._default_bid_generator = BidRequestGenerator()
        self._mobile_bid_generator = MobileBidGenerator()
    def GenerateBidRequest(self):
        random_number = random.random()
        if random_number < self._mobile_proportion:
            return self._mobile_bid_generator.GenerateRequest()
        else:
            return self._default_bid_generator.GenerateRequest()
    
    def GeneratePingRequest(self):
        return self._default_bid_generator.GeneratePingRequest()

class BidRequestGenerator(object):
    def __init__(self):
        pass

    def GenerateRequest(self):
        """
        @note: 生成一个随机请求 
        @return: 生成的请求
        """
        bid_request = baidu_realtime_bidding_pb2.BidRequest()
        bid_request.id = self._GenerateId(ID_LENGTH)
        self._GenerateUserInfo(bid_request)
        self._GeneratePageInfo(bid_request)
        self._GenerateAdslot(bid_request)
        return bid_request

    def GeneratePingRequest(self):
        """
        @note: 生成一个ping 请求，只设置id和 is_ping
        @return: 生成的请求
        """
        bid_request = baidu_realtime_bidding_pb2.BidRequest()
        bid_request.id = self._GenerateId(ID_LENGTH)
        bid_request.is_ping = True
        return bid_request

    def _GenerateId(self,length):
        """
        @note:  按照长度随机生成 数字或者小写字母组成的串
        @return: 生成的随机字符串
        """
        allow_list = string.ascii_lowercase + string.digits
        id = [ random.choice(allow_list) for _ in range(length) ]
        return "".join(id)

    def _GenerateUserInfo(self,bid_request):
        """
        @note: 随机生成用户信息
        @return:
        """
        bid_request.ip = ".".join([ str(random.randint(0,255)) for _ in range(4) ]) 
        bid_request.user_agent = random.choice(USER_AGENTS)
        bid_request.baidu_user_id = self._GenerateId(BAIDU_USER_ID_LENGTH)
        bid_request.baidu_user_id_version = BAIDU_USER_ID_VERSION
        for user_category in self._GenerateSet(USER_CATEGORY, MAX_USER_CATEGORY):
            bid_request.user_category.append(user_category)
        bid_request.gender = random.choice(GENDER)
        bid_request.detected_language = random.choice(LANGUAGE_CODES)
        return
    
    def _GenerateAdslot(self,bid_request):
        """
        @note: 随机生成广告位信息
        @return:
        """
        ad_slot = bid_request.adslot.add()
        ad_slot.ad_block_key = random.randint(1,MAX_AD_BLOCK_KEY) 
        ad_slot.sequence_id = random.randint(1,32)
        ad_slot.width, ad_slot.height = random.choice(AD_SIZES)
        ad_slot.adslot_type = random.choice(ADSLOT_TYPE)

        ad_slot.slot_visibility = random.choice(SLOT_VISIBILITY) 
        num_creative_type = random.randint(1,MAX_CREATIVE_TYPE_NUM)
        for creative in self._GenerateSet(CREATIVE_TYPES,num_creative_type):
            ad_slot.creative_type.append(creative)
        num_excluded_landing_page_url = random.randint(1, MAX_EXCLUDED_LANDING_PAGE_URL_NUM )
        for excluded_url  in self._GenerateSet(LANDING_PAGE_URLS,num_excluded_landing_page_url):
            ad_slot.excluded_landing_page_url.append( excluded_url) 
        ad_slot.minimum_cpm  = random.randint(10,MAX_MINIMUM_CPM)
        return 

    def _GeneratePageInfo(self, bid_request):
        """
        @note: 随机生成页面信息
        @return:  
        """
        bid_request.url = random.choice(URLS) 
        bid_request.referer = random.choice(REFERERS) 
        bid_request.site_category = random.choice(SITE_CATEGORY)
        bid_request.site_quality = random.choice(SITE_QUALITY)
        bid_request.page_type = random.choice(PAGE_TYPE)
        num_page_keyword = random.randint(1,MAX_PAGE_KEYWORD_NUM)
        for keyword in self._GenerateSet(PAGE_KEYWORDS,num_page_keyword):
            bid_request.page_keyword.append(keyword) 
        
        bid_request.page_quality = random.choice(PAGE_QUALITY)
        num_excluded_product_category = random.randint(1,MAX_EXCLUDED_PRODUCT_CATEGORY_NUM)
        for excluded_category in self._GenerateSet(PRODUCT_CATEGORY,num_excluded_product_category):
            bid_request.excluded_product_category.append(excluded_category)
        return 

    def _GenerateSet(self,collection,set_size):
        """
        @note:  根据给定的列表， 从列表中随机选择 几个元素组成一个不重复的集合
        @param: 
            collection: 元素列表 
            set_size: 集合的长度
        @return:    不重复的集合
        """
        unique_collection = set(collection)
        if len(unique_collection) < set_size:
            return unique_collection
        s = set()
        while len(s) < set_size:
            s.add(random.choice(collection))
        return s;

class MobileBidGenerator(BidRequestGenerator):
    """
    @note:生成mobile流量请求
    """
    def __init__(self):
        BidRequestGenerator.__init__(self)

    def GenerateRequest(self):
        """
        @note: 生成mobile请求并返回
        """
        bid_request = baidu_realtime_bidding_pb2.BidRequest()
        bid_request.is_test = True
        bid_request.id = self._GenerateId(ID_LENGTH)
        self._GeneratePageInfo(bid_request)
        self._GenerateUserInfo(bid_request)
        self._GenerateMobile(bid_request)
        self._GenerateGeoInfo(bid_request)
        self._GenerateAdslot(bid_request)
        return bid_request
    
    def _GenerateMobile(self, bid_request):
        """
        @note: 生成mobile结构中的信息
        """
        # Pick a mobile device at random.
        (device_type, platform, os_major_version, os_minor_version, os_micro_version,
        is_app, brand, model, screen_width, screen_height, bid_request.user_agent, 
        wireless_network_type, carrier_id) = random.choice(MOBILE_DEVICE_INFO)

        # Add mobile fields
        mobile = bid_request.mobile
        mobile.carrier_id = carrier_id
        mobile.platform = platform
        mobile.os_version.os_version_major = os_major_version
        mobile.os_version.os_version_minor = os_minor_version
        mobile.os_version.os_version_micro = os_micro_version
        mobile.device_type = device_type
        mobile.screen_width = screen_width
        mobile.screen_height = screen_height
        if is_app:
            self._GenerateAppInfo(bid_request)

    def _GenerateGeoInfo(self, bid_request):
        """
        @note:随机生成位置信息
        """
        user_coordinate = bid_request.user_geo_info.user_coordinate.add()
        user_location = bid_request.user_geo_info.user_location
        #gen coordinate
        (latitude, longitude, standard ) = random.choice(COORDINATE_INFO)
        user_coordinate.latitude = latitude
        user_coordinate.longitude = longitude
        user_coordinate.standard = standard
        #gen location
        (province, city, district, street) = random.choice(USER_LOCATION_INFO)
        user_location.province = province.decode('GBK')
        user_location.street = city.decode('GBK')
        user_location.district = district.decode('GBK')
        user_location.street = street.decode('GBK')
    
    def _GenerateAppInfo(self, bid_request):
        """
        @note:随机生成app信息
        """
        (app_id, app_bundle_id, app_category, app_interaction_type) =  random.choice(MOBILE_APP_INFO)
        
        mobile_app = bid_request.mobile.mobile_app
        mobile_app.app_id = app_id
        mobile_app.app_bundle_id = app_bundle_id
        mobile_app.app_category = app_category
        mobile_app.app_interaction_type.append(app_interaction_type)
