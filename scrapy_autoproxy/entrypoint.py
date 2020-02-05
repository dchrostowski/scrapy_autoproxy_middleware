
import docker
import os

from scrapy_autoproxy.config import config

class Autoproxy(object):

    def __init__(self,config_file=None):
        self.client = docker.from_env()
        
        self.config = config
        if config_file:
            self.config = config(config_file)


    def start_docker_containers(self):
        
        data_dir = os.path.join(__file__,'data')
        env_file = os.path.join(data_dir,'.env')
        docker_compose_file = os.path.join(data_dir,'docker-compose.yml')

        if os.path.isdir(data_dir):

            if os.path.isfile(env_file):
                self.config.write_to_env_file(env_file)
            
            if os.path.isfile(docker_compose_file_):
                os.system('cd %s && docker-compose up -d' % data_dir)

            
        
        

