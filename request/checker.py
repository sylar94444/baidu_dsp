# -*- coding: utf-8 -*-
#!/usr/bin/env python
#Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved 
import datetime
import optparse
import os
import random
import threading
import time

from  check_sender import *


def CreateRequesters(num_senders, max_qps, bfp_url, bfp_c, 
        seconds=0, requests=0, interval=0):
    """
    @note: 创建 线程， 每个线程创建sender.HTTPSender和feeder.HTTPSender对象
    @params:
    num_senders: 创建的线程数 
    max_qps: 最大的qps 
    url: 竞价的rtb_url 
    logger_obj: 日志对象 
    seconds: 每个持续发送的时间 
    requests: 每个发送的请求个数 
    @return: 返回创建的所有线程
    """
    #import pdb
    #pdb.set_trace()
    seconds = seconds or 0
    requests = requests or 0
    # Create at most max_qps/10 threads, giving each thread at least 10 QPS.
    num_senders = min(num_senders, max_qps)
    num_senders = max(num_senders, 1)  # Avoid setting num_senders to 0.
    send_rate_per_sender = num_senders / float(max_qps)
    requests_per_sender = requests / num_senders
    requesters = []
    for i in xrange(num_senders):
        requester = Requester(bfp_url, bfp_c,
                          send_rate_per_sender, seconds, requests_per_sender)
        requester.name = 'requester-thread-%d' % i
        requesters.append(requester)
    return requesters


class Requester(threading.Thread):
    def __init__(self, bfp_url, bfp_c, 
               time_between_requests, seconds=None, requests=None):
        """
        @note: 线程初始化
        @params:
            time_between_requests: 每秒发送的请求数 
            seconds: 持续发送的时间 
            requests: 发送请求的总数 
        @raises: ValueError: If none or both of seconds and requests are specified.
        """
        super(Requester, self).__init__()
        self._bfp_url = bfp_url 
        self._bfp_c = bfp_c 
        self._time_between_requests = float(time_between_requests)
        self._generated_requests = 0
        self._last_request_start_time = 0.0
        if ((seconds and requests) or
            (not seconds and not requests)):
            raise ValueError('Exactly one of seconds and requests must be'
                       ' specified')
        if seconds:
            self._timedelta = float(seconds)
            self._use_requests_as_stop_signal = False
        else:
            self._max_requests = requests
            self._use_requests_as_stop_signal = True

    def run(self):
        self.Start()

    def Start(self):
	tmp = 0
        #import pdb
        #pdb.set_trace()
        if not self._use_requests_as_stop_signal:
            self._start_time = self._GetCurrentTime()
            self._stop_time = self._start_time + self._timedelta
            #print "start" + str(self._stop_time)

        while self._ShouldSendMoreRequests():
            request_start_time = self._GetCurrentTime()
            try:
                #import pdb
                #pdb.set_trace()
                #print "begin"
                sender = HTTPSender()
                status, data = sender.Send_url(self._bfp_c, self._bfp_url)
            except:
                status = 404
                data = ""
            finally:
                pass
            tmp = tmp +1
            self._Wait()
            self._last_request_start_time = request_start_time

    def _Wait(self):
        time_to_wait = self._time_between_requests
        if self._last_request_start_time:
            time_since_last_request = (self._GetCurrentTime() -
                                 self._last_request_start_time)
            time_to_wait = max(0, time_to_wait - time_since_last_request)

        if time_to_wait:
            time.sleep(time_to_wait)

    def _ShouldSendMoreRequests(self):
        if self._use_requests_as_stop_signal:
            return self._generated_requests < self._max_requests
        else:
            return self._GetCurrentTime() < self._stop_time

    def _GetCurrentTime(self):
        a = time.time()
        #print "cur"+ str(a)
        return a


def SetupCommandLineOptions():
    parser = optparse.OptionParser()
    parser.add_option('--url', help='URL of the bidder.')
    parser.add_option('--max_qps', type='int',
                    help='Maximum queries per second to send to the bidder.')
    parser.add_option('--seconds', type='int',
                    help='Total duration in seconds. Specify exactly one of '
                    '--seconds or --requests.')
    parser.add_option('--requests', type='int',
                    help='Total number of requests to send. Specify exactly '
                    'one of --seconds or --requests.')
    parser.add_option('--num_threads', type='int',
                    default=40, help='Maximum number of threads to use. The '
                    'actual number of threads may be lower.')
    return parser


def ParseCommandLineArguments(parser):
    opts, args = parser.parse_args()
    if args:
        parser.error('unexpected positional arguments "%s".' % ' '.join(args))
    if ((opts.requests and opts.seconds) or
      (not opts.requests and not opts.seconds)):
        parser.error('exactly one of --requests and --seconds requires a value.')
    if not opts.url:
        parser.error('--url requires a value.')
    if not opts.max_qps:
        parser.error('--max_qps requires a value.')
    return opts



def main():
    bfp_c='/ecom?di=u24179&dcb=BAIDU_CPRO_SETJSONADSLOT&dtm=BAIDU_CPRO_SETJSONADSLOT&dai=1&jn=3&ltu=www.youdi.com&liu=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/all_300_250.html&ltr=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/test.html&ps=18x18&psr=1280x800&par=1280x770&pcs=1280x205&pss=1280x36&pis=1280x205&cfv=11&ccd=32&col=zh-CN&cec=GBK&tpr=1375946930330&kl=&dis=16'
    parser = SetupCommandLineOptions()
    opts = ParseCommandLineArguments(parser)
    requesters = CreateRequesters(opts.num_threads, opts.max_qps, opts.url, bfp_c
                                , opts.seconds, opts.requests )
    for requester in requesters:
        requester.start()

    for requester in requesters:
        requester.join()

def check(url, qps, seconds):
    bfp_c='?di=u24179&dcb=BAIDU_CPRO_SETJSONADSLOT&dtm=BAIDU_CPRO_SETJSONADSLOT&dai=1&jn=3&ltu=www.youdi.com&liu=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/all_300_250.html&ltr=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/test.html&ps=18x18&psr=1280x800&par=1280x770&pcs=1280x205&pss=1280x36&pis=1280x205&cfv=11&ccd=32&col=zh-CN&cec=GBK&tpr=1375946930330&kl=&dis=16'

    requesters = CreateRequesters(100, qps, url, bfp_c
                                , seconds, 0)

    for requester in requesters:
        requester.start()

    for requester in requesters:
        requester.join()

if __name__ == '__main__':
    main()
