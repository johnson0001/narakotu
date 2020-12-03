import MySQLdb
from sshtunnel import SSHTunnelForwarder

# EC2 Instance
SSH_HOST = '54.150.164.141'
SSH_PORT = 22
SSH_HOST_KEY = None
SSH_USERNAME = 'ec2-user'
SSH_PASSWORD = None
SSH_PKEY = '/Users/yuji-ka/.ssh/myKey.pem'
# Database Instance
DB_HOST = 'database-1.c3257crsgfit.ap-northeast-1.rds.amazonaws.com'
DB_PORT = 3306
REMOTE_BIND_ADDRESS = (DB_HOST, DB_PORT)
DB_USER = 'admin'
DB_PASSWORD = 'L1lWeRq9tn9pV60FSyQY'
DB_NAME = 'testdb'
LOCAL_HOST = '127.0.0.1'
DB_CHARSET = 'utf8'

# Access Server by SSH
server = SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_host_key=SSH_HOST_KEY,
    ssh_username=SSH_USERNAME,
    ssh_password=SSH_PASSWORD,
    ssh_pkey=SSH_PKEY,
    remote_bind_address=REMOTE_BIND_ADDRESS
)
server.start()

# Connect Database from Server
connection = MySQLdb.connect(
    host=LOCAL_HOST,
    port=server.local_bind_port,
    user=DB_USER,
    db=DB_NAME,
    passwd=DB_PASSWORD,
    charset=DB_CHARSET
)
cursor = connection.cursor()

# Write down Query
id1 = 12
name1 = 'yuji'
try:
    sql = "insert into user values (3, 'yamada')"
    # cursor.execute(sql)
    cursor.execute("insert into user values (%s, %s)", (id1, name1))
    cursor.execute("SELECT * FROM user")
    for row in cursor.fetchall():
        print(row)
except MySQLdb.Error as e:
    print('MySQLdb.Error: ', e)
connection.commit() # Save

# Close Connection and Server
connection.close()
server.stop()