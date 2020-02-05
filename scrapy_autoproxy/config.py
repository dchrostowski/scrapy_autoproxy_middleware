import os
import configparser

CUR_DIR, _ = os.path.split(__file__)
CONFIG_FILE = os.path.join(CUR_DIR,'data','config','autoproxy.cfg')
ENV_FILE = os.path.join(CUR_DIR,'data','.env')


class AutoproxyConfig(dict):

    def get_config_section(self,section):
        return {key: self.parser.get(section,key) for key in self.parser.options(section)}

        
        #return { config_key: parsed['redis'][config_key] for config_key in redis_config_keys }

    def __init__(self,config_file=CONFIG_FILE):

        if os.path.isfile(config_file):
            self.parser = configparser.ConfigParser()
            self.parser.read(config_file)
            
            config_dict = {
                    'redis_config': self.get_config_section('redis'),
                    'db_config': self.get_config_section('database'),
                    'settings': self.get_config_section('settings')
            }
            
            
            self.update(config_dict)

            for k,v in self.items():
                setattr(self,k,v)


    def get_env_dict(self):
        return  {   'POSTGRES_HOST'    : self.db_config['host'],
                     'POSTGRES_USER'    : self.db_config['user'],
                     'POSTGRES_DATABASE': self.db_config['database'],
                     'POSTGRES_PASSWORD': self.db_config['password'],
                     'POSTGRES_PORT'    : self.db_config['port'],
                     'REDIS_HOST'       : self.redis_config['host'],
                     'REDIS_PORT'       : self.redis_config['port'],
                     'REDIS_PASSWORD'   : self.redis_config['password']
                }

    
    def get_env_lines(self):
        return ["%s=%s" % (k,v) for k,v in self.get_env_dict().items() ]

        
        
    def write_to_env_file(self,outfile):
        with open(outfile,'w') as ofh:
            for line in self.get_env_lines():
                ofh.write("%s\n" % line)

    @classmethod
    def config(cls,config_file):
        return cls(config_file)


        
    
config = AutoproxyConfig()

if __name__ == "__main__":
    if os.path.isfile(ENV_FILE):
        config.write_to_env_file(ENV_FILE)

