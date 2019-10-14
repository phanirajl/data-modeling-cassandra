from cassandra.cluster import Cluster
from sql_queries import list_drop_tables, list_create_tables

def insertfromdataframe(IPCluster, keyspace, table, dataframe):
    cluster, session = keyspace_connection(IPCluster, keyspace)

    for index, row in dataframe.iterrows():
        try:
            session.execute('''INSERT INTO ''' + table + '''
            VALUES (''' + ','.join(['%s' for x in row]) + ''');''',
        row)
        except Exception as e:
            print("Error execute query")
            print(e)

    session.shutdown()
    cluster.shutdown()

def keyspace_connection(IPCluster, keyspace):
    try:
        cluster = Cluster([ IPCluster ])
    except cluster.Error as e:
        print("Error cluster connection")
        print(e)

    try:
        session = cluster.connect()
        session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkifydb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };""")
        session.set_keyspace(keyspace)
    except Exception as e:
        print("Error init session")
        print(e)

    return cluster, session

def main(IPCluster, keyspace):
    cluster, session = keyspace_connection(IPCluster, keyspace)

    for i_drop in list_drop_tables:
        session.execute(i_drop)

    for i_create in list_create_tables:
        session.execute(i_create)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main("127.0.0.1", "sparkifydb")