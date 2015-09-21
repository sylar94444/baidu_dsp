# -*- coding: utf-8 -*-
#!/usr/bin/env python
#Copyright (c) 2013 Baidu.com, Inc. All Rights Reserved 

import httplib
import urlparse

CONTENT_TYPE = "application/octet-stream"
CONTENT_TYPE_HEADER = "Content-type"


class HTTPSender(object):
  def __init__(self, url):
    parsed = urlparse.urlparse(url)
    # Set some defaults.
    self._port = '80'
    self._path = '/'
    self._connect_timeout = 1000000
    self._read_timeout = 1000000 

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

  def __call__(self, *args):
    return self.Send(*args)

  def Send(self, payload):
    if not self._connection:
        self._connection = httplib.HTTPConnection(self._host, self._port)
	#self._connection.set_debuglevel(5)
    self._connection.request('POST', self._path, payload,
                             {CONTENT_TYPE_HEADER: CONTENT_TYPE})
    self._connection.sock.settimeout(self._read_timeout)
    response = self._connection.getresponse()
    status = response.status
    data = response.read()
    self._connection.close()
    return (status, data)

if __name__ == '__main__':
    hs = HTTPSender('http://tsm_liaozhenliang.baidu.com/TsmDspService/Bid?url=howardliao')
    import pdb
    pdb.set_trace()