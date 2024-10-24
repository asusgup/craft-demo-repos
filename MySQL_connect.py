from jproperties import Properties
import mysql.connector


configs = Properties()
with open('mysql-config.properties', 'rb') as config_file:
		 configs.load(config_file)


def build_Mysql_Connection():
	global configs
	try:
		connection = mysql.connector.connect(host=configs.get("mysql_host").data,
		                                     database=configs.get("mysql_report_database").data,
		                                     user=configs.get("mysql_user").data,
		                                     password=configs.get("mysql_password").data)
		if connection.is_connected():
			print(connection)
			db_Info = connection.get_server_info()
			print("Connected to MySQL Server version ", db_Info)
			return connection, connection.cursor()
	except Exception as err:
	   print("Connection to MySQL DB failed. ERROR - "+ str(err))	
	finally:
		pass   	
