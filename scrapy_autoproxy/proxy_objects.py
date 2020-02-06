

class Proxy(object):
    def __init__(self,*args,**kwargs):
        pass


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