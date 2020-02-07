

class Proxy(object):
    AVAILABLE_PROTOCOLS = ('http', 'https', 'socks5', 'socks4')

    def __init__(self, address, port, protocol='http', proxy_id=None, proxy_key=None):
        self.address = address
        self.port = int(port)
        protocol = protocol.lower()
        if protocol not in self.__class__.AVAILABLE_PROTOCOLS:
            raise Exception("Invalid protocol %s" % protocol)
        self.protocol = protocol
        ifn = lambda x: int(x) if x is not None else None
        self.proxy_id = ifn(proxy_id)
        self._proxy_key = proxy_key
        

    def urlify(self):
        return "%s://%s:%s" % (self.protocol, self.address, self.port)

    def id(self):
        return self.proxy_id

    @property
    def proxy_key(self):
        if self._proxy_key is None and self.proxy_id is not None:
            self._proxy_key = "%s_%s" % ('p',self.proxy_id)
        return self._proxy_key
        
    @proxy_key.setter
    def proxy_key(self,pkey):
        self._proxy_key = pkey

    def to_dict(self,redis_format=False):
        obj_dict = {
            "address": self.address,
            "port":  str(self.port),
            "protocol": self.protocol,
        }

        if self.proxy_id is not None:
            obj_dict.update({'proxy_id': self.proxy_id})
        return obj_dict


class Queue(object):
    def __init__(self, domain, queue_id=None, queue_key=None):
        self.domain = domain
        ifn = lambda x: int(x) if x is not None else None
        self.queue_id = ifn(queue_id)
        self._queue_key = queue_key

    def id(self):
        return self.queue_id

    @property
    def queue_key(self):
        if self._queue_key is None and self.queue_id is not None:
            self._queue_key = "%s_%s" % (QUEUE_PREFIX,self.queue_id)
        return self._queue_key
            
    @queue_key.setter
    def proxy_key(self,qkey):
        self._queue_key = qkey
    
    def to_dict(self, redis_format=False):
        obj_dict = {
            "domain": self.domain,
        }

        if(self.queue_id is not None):
            obj_dict.update({"queue_id": self.queue_id})
        
        return obj_dict


class Detail(object):
    def __init__(self,*args,**kwargs):
        pass

class ProxyObject(object):
    def __init__(self,*args,**kwargs):
        pass