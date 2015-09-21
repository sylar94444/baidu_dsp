# -*- coding: utf-8 -*-
#!/usr/bin/env python
#Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved 

import httplib
import urlparse
from frame.lib.commonlib.xtslog import xtslog 
CONTENT_TYPE = "application/octet-stream"
CONTENT_TYPE_HEADER = "Content-type"


class HTTPSender(object):
    def init(self, url):
        parsed = urlparse.urlparse(url)
        # Set some defaults.
        self._port = '80'
        self._path = '/'

        if not parsed[0] and parsed[0] != 'http':
            raise ValueError("URL scheme must be HTTP.")
        if not parsed[1]:
            raise ValueError("URL must have a hostname.")

        netloc = parsed[1]
        self._host = netloc
        # Try to find a valid port, otherwise assume the netloc is just the
        # hostname.
        if netloc.find(":") != -1:
            components = netloc.rsplit(":", 1)
            if len(components) == 2:
                host_str = components[0]
                port_str = components[1]
                int(port_str)
                # Valid numerical port, set host and port accordingly
                self._port = port_str
                self._host = host_str

        if (parsed[2] or parsed[3] or parsed[4] or parsed[5]):
            self._path = urlparse.urlunparse(('', '', parsed[2], parsed[3],
                                        parsed[4], parsed[5]))
        self._connection = None

    def Get(self, url):
        try:
            self.init(url)
            if not self._connection:
                 if self._port == '80':
                     self._connection = httplib.HTTPConnection(self._host)
                 else:
                     self._connection = httplib.HTTPConnection(self._host, self._port)
            
            self._connection.request('GET', self._path,headers={"Cookie":"BAIDUID=1C28E8F769E41C8AE695D327468BC3A6:FG=1","User-Agent":"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1) Gecko/20090624 Firefox/3.5","Accept": "text/plain"})
            response = self._connection.getresponse()
            
            status = response.status
            data = response.read()
            header = response.getheader('location')
            self._connection.close()
            xtslog.info('check status : ' + str(status) + '\t' + str(url))
            
            if status == 302 or status == 301:
                re_status = self.Get(header)
        except Exception, e:
            xtslog.info("Failed", url)
            print "failed " + url + " " + str(e)
            return 0,0,0
        return (status, url, data)

    def geturl(self,pattern, orig_str):
        import re
        pattern = '"%s.*' %(pattern)
        result = re.search(pattern,orig_str)
        if result != None:
            return result.group().replace("&nbsp;"," ")
        else:
            return ""
 
    def getck_xx(self,orig_str):
        li=orig_str.splitlines()
        #print li
        result=''
        for li_one in li:
	    if li_one.startswith('"curl"'):
		#print li_one
                result=li_one[li_one.find("http"):-2]
		#print result
        return result 

    def Send_url(self, bfp_c, bfp_url):
        try:
            result = self.Get(bfp_url + bfp_c)[2]
            xtslog.info(result)
            qn_pos = result.find('qn')
            if qn_pos == -1:
                return 0, 0
            qn_num = result[qn_pos + 7 : qn_pos + 23]
            adcacheurl='http://cq01-testing-cbbs-vir06.vm.baidu.com:8098/bfp/snippetcacher.php?qn='
            if qn_num == "CPRO_SETJSONADSL":
                return 0,0
            tmp_result = self.Get(adcacheurl + qn_num)[2]
            #stat = self.Get(self.getck_xx(tmp_result))
            result = self.geturl("curl",tmp_result) + self.geturl("noticeurl",tmp_result)
            result = "{" + result + "}"
            result_dict = eval(result)
            for one in result_dict["noticeurl"]:
                stat_0 = self.Get(one)
                xtslog.info('send win notice url: %s %s'%(str(stat_0[0]), str(one)))
            import random
            if random.randint(0,100) <10:
                stat = self.Get(result_dict["curl"])
                xtslog.info('send click url: %s %s'%(str(stat[0]), str(result_dict["curl"])))
        except:
            return 0,0

if __name__ == '__main__':
    sender = HTTPSender()
    bfp_c='/ecom?di=u24179&dcb=BAIDU_CPRO_SETJSONADSLOT&dtm=BAIDU_CPRO_SETJSONADSLOT&dai=1&jn=3&ltu=www.youdi.com&liu=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/all_300_250.html&ltr=http://cq01-rdqa-dev051.cq01.baidu.com:8031/next/test.html&ps=18x18&psr=1280x800&par=1280x770&pcs=1280x205&pss=1280x36&pis=1280x205&cfv=11&ccd=32&col=zh-CN&cec=GBK&tpr=1375946930330&kl=&dis=16'
    bfpurl = 'http://ai-cm-hpbak010.ai01.baidu.com:8999'
    for i in range(2):
        sender.Send_url(bfp_c, bfpurl)
