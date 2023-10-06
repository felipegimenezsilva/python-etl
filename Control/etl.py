import time
import Control.database

class ExtractTransformLoad:

    def __init__(self):
        self.etl = {}
        self.databases = Control.database.Database()
    
    def add(self,title,etl_obj):
        if title in self.etl.keys(): raise Exception("Duplicated ETL title:", title)
        self.etl[title] = etl_obj
        return self
    
    def list(self):
        return list(self.etl.keys())
    
    def get(self,title):
        if not title in self.etl.keys: raise Exception("ETL not declared:", title)
        return self.etl[title]

    def run(self):
        resume = { 'data' : time.time(), 'process' : {}}
        for title, executor in self.etl.items():
            infos = { } 
            timer_start = time.time()
            print(f"{title}: Running")
            try: executor( self.databases )
            except Exception as e: infos['error'] = str(e)
            timer_end = time.time()
            infos['delay']= timer_end-timer_start
            infos['status'] = 'ok' if not 'error' in infos.keys() else 'error'
            resume['process'][title]=infos
        return resume
