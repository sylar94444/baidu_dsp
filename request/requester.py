# -*- coding: utf-8 -*-
#!/usr/bin/env python
#Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved 
import datetime
import optparse
import os
import random
import threading
import time

import generator
import log
import sender

GOOD_LOG_TEMPLATE = 'good-%s.log'
PROBLEMATIC_LOG_TEMPLATE = 'problematic-%s.log'
INVALID_LOG_TEMPLATE = 'invalid-%s.log'
ERROR_LOG_TEMPLATE = 'error-%s.log'


def CreateRequesters(num_senders, max_qps, url, logger_obj, 
        seconds=0, requests=0, mobile_proportion=0 ):
    """
    @note: 创建 线程， 每个线程创建sender.HTTPSender和feeder.HTTPSender对象
    @params:
    num_senders: 创建的线程数 
    max_qps: 最大的qps 
    url: 竞价的rtb_url 
    logger_obj: 日志对象 
    seconds: 每个持续发送的时间 
    requests: 每个发送的请求个数
    mobile_proportion: 移动流量比例(0-100)
    @return: 返回创建的所有线程
    """
    seconds = seconds or 0
    requests = requests or 0
    # Create at most max_qps/10 threads, giving each thread at least 10 QPS.
    num_senders = min(num_senders, max_qps / 10)
    num_senders = max(num_senders, 1)  # Avoid setting num_senders to 0.
    send_rate_per_sender = num_senders / float(max_qps)
    requests_per_sender = requests / num_senders
    requesters = []
    for i in xrange(num_senders):
        generator_obj = generator.BidGeneratorManager(mobile_proportion)
        sender_obj = sender.HTTPSender(url)
        requester = Requester(generator_obj, logger_obj, sender_obj,
                          send_rate_per_sender, seconds, requests_per_sender)
        requester.name = 'requester-thread-%d' % i
        requesters.append(requester)
    return requesters


class Requester(threading.Thread):
    def __init__(self, generator_obj, logger_obj, sender_obj,
               time_between_requests, seconds=None, requests=None):
        """
        @note: 线程初始化
        @params:
            generator_obj: 生成随机请求的对象 
            logger_obj: 统计日志的对象 
            sender_obj: 发送请求的对象 
            time_between_requests: 每秒发送的请求数 
            seconds: 持续发送的时间 
            requests: 发送请求的总数 
        @raises: ValueError: If none or both of seconds and requests are specified.
        """
        super(Requester, self).__init__()
        self._generator = generator_obj
        self._logger = logger_obj
        self._sender = sender_obj
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

        while self._ShouldSendMoreRequests():
            request = self._GenerateRequest()
            payload = request.SerializeToString()
            request_start_time = self._GetCurrentTime()
            try:
                status, data = self._sender(payload)
            except:
                status = 404
                data = ""
            finally:
                self._logger.LogSynchronousRequest(request, status, data)
            print tmp
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

    def _GenerateRequest(self):
        if random.random() < 0.01:
            bid_request = self._generator.GeneratePingRequest()
        else:
            bid_request = self._generator.GenerateBidRequest()
        self._generated_requests += 1
        return bid_request

    def _GetCurrentTime(self):
        return time.time()


def PrintSummary(logger):
    """
    @note: 打印统计的信息
    @param: 日志统计的对象 
    """
    logger.Done()
    summarizer = log.LogSummarizer(logger)
    summarizer.Summarize()
    timestamp = str(datetime.datetime.now())
    timestamp = timestamp.replace(' ', '-', timestamp.count(' '))
    timestamp = timestamp.replace(':', '', timestamp.count(':'))
    good_log_filename = GOOD_LOG_TEMPLATE % timestamp
    good_log = open(good_log_filename, 'w')
    problematic_log_filename = PROBLEMATIC_LOG_TEMPLATE % timestamp
    problematic_log = open(problematic_log_filename, 'w')
    invalid_log_filename = INVALID_LOG_TEMPLATE % timestamp
    invalid_log = open(invalid_log_filename, 'w')
    error_log_filename = ERROR_LOG_TEMPLATE % (timestamp)
    error_log = open(error_log_filename, 'w')
    summarizer.WriteLogFiles(good_log, problematic_log, invalid_log, error_log)
    good_log.close()
    problematic_log.close()
    invalid_log.close()
    error_log.close()
    summarizer.PrintReport()

    # Cleanup by deleting empty files.
    for file_name in [good_log_filename, problematic_log_filename,
          invalid_log_filename, error_log_filename]:
        if not os.path.getsize(file_name):
            os.remove(file_name)


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
    parser.add_option('--mobile_proportion', type='float',
                    default=0.2,
                    help='Proportion of requests that are for mobile slots '
                    '(0.2 by default).')
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
    parser = SetupCommandLineOptions()
    opts = ParseCommandLineArguments(parser)
    logger_obj = log.Logger()
    requesters = CreateRequesters(opts.num_threads, opts.max_qps, opts.url,
                                logger_obj, opts.seconds, opts.requests, opts.mobile_proportion)
    for requester in requesters:
        requester.start()

    for requester in requesters:
        requester.join()

    PrintSummary(logger_obj)

def rpyc(url, requests,qps=1):
    logger_obj = log.Logger()
    requesters = CreateRequesters(1, qps, url,
                                logger_obj, 0 , requests )
    for requester in requesters:
        requester.start()

    for requester in requesters:
        requester.join()
    logger_obj.Done()
    summarizer = log.LogSummarizer(logger_obj)
    summarizer.Summarize()
    return summarizer 


if __name__ == '__main__':
    main()
