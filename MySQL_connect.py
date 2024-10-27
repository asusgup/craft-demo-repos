from jproperties import Properties
import mysql.connector
import logging


configs = Properties()
with open('mysql-config.properties', 'rb') as config_file:
		 configs.load(config_file)


def build_Mysql_Connection():
	global configs
	try:
		logging.info("Initialising mysql connector")
		connection = mysql.connector.connect(host=configs.get("mysql_host").data,
		                                     database=configs.get("mysql_report_database").data,
		                                     user=configs.get("mysql_user").data,
		                                     password=configs.get("mysql_password").data)
		if connection.is_connected():
			logging.info(connection)
			db_Info = connection.get_server_info()
			logging.info("Connected to MySQL Server version ", db_Info)
			return connection, connection.cursor()
	except Exception as err:
	   logging.error("Connection to MySQL DB failed. ERROR - "+ str(err))	
	finally:
		pass   	
