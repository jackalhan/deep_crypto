import pymysql
import pymysql.cursors
from pymysql import Error

from utility.config_parser import get_config

__dbconf={}
def create():
    """ Connect to MySQL database """
    conn = None
    try:

        dbconf = get_config(config_type='db', section='mysql')

        conn = pymysql.connect(host= dbconf['host'],
                               database=dbconf['database'],
                               user=dbconf['user'],
                               password=dbconf['password'],
                               port = int(dbconf['port']),
                               autocommit=True)
        print('Connected to MySQL database')
    except Error as e:
        print(e)

    return conn

def disconnect(conn):
    try:
        conn.close()
        print('MySQL database is disconnected')
    except Error as e:
        print(e)

# if __name__ == '__main__':
#     conn = connect()
#     disconnect(conn)

# conn= pymysql.connect(host='localhost',user='user',password='user',db='testdb',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
# a=conn.cursor()
# sql='CREATE TABLE `users` (`id` int(11) NOT NULL AUTO_INCREMENT,`email` varchar(255) NOT NULL,`password` varchar(255) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;'
# a.execute(sql)