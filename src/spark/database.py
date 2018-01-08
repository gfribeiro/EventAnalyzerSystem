# database.py
from cassandra.cluster import Cluster
import json

class CassandraConnection():
    
    def __init__(self,chost,ckeyspace):
        self.CASSANDRA_HOST = chost
        self.KEYSPACE = ckeyspace
        
    
    def getSession(self):
        cassandra_cluster = Cluster([self.CASSANDRA_HOST])
        cassandra_session = cassandra_cluster.connect(self.KEYSPACE)
        return cassandra_session

class DataManager():

    def __init__(self):
        self.db_config = json.load(open('dbconfig.json'))
        self.db_session = CassandraConnection(self.db_config["cassandra"]["host"],self.db_config["cassandra"]["keyspace"]).getSession()
    
    def getAppConfig(self):
        rows = self.db_session.execute('SELECT * FROM app_config')
        config = {}
        for row in rows:
            config[row.config_key] = row.config_value
        return config

    def getEventCategories(self):
        return self.db_session.execute('SELECT * FROM event_category')

    def getMonitoringKeywords(self):
        rows = self.getEventCategories()
        keywords = ''
        for row in rows:
            keywords += row.keywords + ','
        return keywords[:-1]

    def insertJson(self,table,json):
        queryString = "INSERT INTO {} JSON '{}'".format(table,json)
        self.db_session.execute(queryString)

if __name__ == "__main__":
    dataManager = DataManager()
    config = dataManager.getAppConfig()
    print(str(config['APP_NAME']))