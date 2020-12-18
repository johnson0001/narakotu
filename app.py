import sys
sys.path.append('/home/pi/.local/lib/python3.7/site-packages')
import MySQLdb
from sshtunnel import SSHTunnelForwarder
import bluepy
import micropyGPS
import serial
import datetime
import time
import pandas as pd
import csv

# Raspberry ID
RASP_ID = 1000

# EC2 Instance
SSH_HOST = '18.183.92.218'
SSH_PORT = 22
SSH_HOST_KEY = None
SSH_USERNAME = 'ec2-user'
SSH_PASSWORD = None
SSH_PKEY = '/home/pi/Documents/kanamitsu/narakotu/app/.ssh/narakotu-ssh-key.pem'
# Database Instance
DB_HOST = 'narakotu-db.cdu3s94jmhjr.ap-northeast-1.rds.amazonaws.com'
DB_PORT = 3306
REMOTE_BIND_ADDRESS = (DB_HOST, DB_PORT)
DB_USER = 'admin'
DB_PASSWORD = 'vg4nWZFqsM4lhsVVBZ4M'
DB_NAME = 'narakotu'
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


### Get Data ###
COCOA_UUID = "0000fd6f-0000-1000-8000-00805f9b34fb"
scanner = bluepy.btle.Scanner(0)
gps = micropyGPS.MicropyGPS(9, 'dd')

while True:
	# GPS Data
	try:
		s = serial.Serial('/dev/serial0', 9600, timeout=1000)
		s.readline()
		sentence = s.readline().decode('utf-8')
		if sentence[0] != '$': # 先頭が$でなければ捨てる
			continue
		for x in sentence:
			gps.update(x) # gps data
	except:
		pass
		
	# COCOA Data
	deviceNum = 0
	cocoaNum = 0
	columns = ['adress', 'rssi', 'uuid']
	df = pd.DataFrame(
		columns=columns)
	devices = scanner.scan(5)
	for device in devices:
		# Make CSV Data
		csvData = []
		csvData.append(device.addr)
		csvData.append(device.rssi)
		for (adtype, desc, value) in device.getScanData():
			if value == COCOA_UUID:
				csvData.append(value)
		if len(csvData) < 3:
			csvData.append('null')			
			
		df2 = pd.DataFrame(
			[csvData],
			columns=columns)
		df = df.append(df2, ignore_index=True)
		
		# get COCOA Data
		if device.rssi > -70: # 受信範囲指定
			for (adtype, desc, value) in device.getScanData():
				if value == COCOA_UUID:
					cocoaNum += 1
			deviceNum += 1
			
	# Time Data	
	dt_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	dt_now2 = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
	
	# Make CSV File
	stampData = []
	stampData.extend([dt_now2, gps.latitude[0], gps.longitude[0]])
	filePath = 'log_data/' + dt_now2 + '.csv'
	with open(filePath, 'w') as f:
		writer = csv.writer(f)
		writer.writerow(stampData)
	df.to_csv(filePath, mode='a')
		
	# Write down Query
	try:	
		cursor.execute("insert into test (rasp_time, db_time, cocoa_num, ble_num, latitude, longitude, raspberry_id) value (%s, now(), %s, %s, %s, %s, %s)", (dt_now, cocoaNum, deviceNum, gps.latitude[0], gps.longitude[0], RASP_ID))
		print("------INSERT DATA------")
		print("Datetime: {}".format(dt_now))
		print("Cocoa Number: {}".format(cocoaNum))
		print("BLE Device: {}".format(deviceNum))
		print("GPS latitude: {} longitude: {}".format(gps.latitude[0], gps.longitude[0]))
	except:
		pass
		
	connection.commit() # Save

	time.sleep(10)

# Close Connection and Server
connection.close()
server.stop()
