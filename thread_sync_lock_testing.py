import threading


def getRunningThreads():
    running = 0
    for t in threading.enumerate():
        print(t)
        if t.isAlive():
            running +=1
    return running

def worker():
    print(threading.currentThread().getName(), "starting")
    time.sleep(random.randint(1,10))
    pm = ProxyManager()
    for i in range(5):
        url = random.choice(test_sites)
        print(threading.currentThread().getName(), "crawling %s" % url)
        proxy = pm.get_proxy(url)
        time.sleep(random.randint(1,12))
        success = random.choice(crawl_statuses)
        print(threading.currentThread().getName(), "crawl success=%s" % success)
        if success:
            successful[url] += 1
        else:
            failures[url] +=1
        proxy.callback(success=success)
        time.sleep(random.randint(1,6))

    print(threading.currentThread().getName(), "stopping")
    return

def make_workers():
    workers = []
    for i in range(5):
        worker_name = "worker_%s" % i
        wkr = threading.Thread(name=worker_name, target=worker)
        workers.append(wkr)
    return workers

workers = make_workers()

def daemon():
    print(threading.currentThread().getName(), 'Starting daemon.')
    for w in workers:
        w.start()
    time.sleep(15)
    while(True):
        scoreboard()
        if getRunningThreads() == 1:
            break
        time.sleep(5)
    sm = StorageManager()
    sm.sync_to_db()
    return scoreboard()
    
    
    
    #print(threading.currentThread().getName(),'stopping daemon')
    #for w in workers:
    #    w.join()


dmn = threading.Thread(name='daemon', target=daemon)
dmn.setDaemon = True
dmn.start()