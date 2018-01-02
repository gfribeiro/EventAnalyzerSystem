# database.py
from cassandra.cluster import Cluster

CASSANDRA_HOST = '172.17.0.3'
CASSANDRA_KEYSPACE = 'sea'

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
        self.db_session = CassandraConnection(CASSANDRA_HOST,CASSANDRA_KEYSPACE).getSession()
    
    def getAppConfig(self):
        rows = self.db_session.execute('SELECT * FROM app_config')
        config = {}
        for row in rows:
            config[row.config_key] = row.config_value
        return config

    def getMonitoringKeywords(self):
        rows = self.db_session.execute('SELECT * FROM event_category')
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