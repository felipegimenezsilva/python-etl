class Database():

    def __init__(self):
        self.databases = {}

    def add(self,title,db_object):
        if title in self.databases.keys(): raise Exception("Duplicated database title:",title)
        self.databases[title] = db_object
        return self
    
    def list(self):
        return list(self.databases.keys())
    
    def get(self,title):
        if not title in self.databases.keys(): raise Exception("Database not declared:", title)
        return self.databases[title]



