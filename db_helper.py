import config
import mysql.connector as connector
from mysql.connector import errorcode
import json

class db_helper():
	def __init__(self):
		self.cfg = config.get_config_section_dict("config.ini","db")
		self.DB_NAME = self.cfg["database"]
		self.connect = None
		self.TABLES = {}

	def connect_to_db(self):
		user = self.cfg["user"]
		password = self.cfg["password"]
		host = self.cfg["host"]
		print user,password,host
		self.connect = connector.connect(user=user, password=password, host=host)
	
	def setup(self, date):
		self.connect_to_db()
		self.use_database()
		self.get_ddl(date)
		
	def close(self):
		self.connect.close()
	
	def create_database(self):
		cursor = self.connect.cursor()
		try:
			cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
		except connector.Error as err:
			print("Failed creating database: {}".format(err))
			exit()
	
	def use_database(self):
		cursor = self.connect.cursor()
		try:
			self.connect.database = self.DB_NAME
		except connector.Error as err:
			if err.errno == errorcode.ER_BAD_DB_ERROR:
				self.create_database()
				self.connect.database = self.DB_NAME
			else:
				print(err)
				exit()
		cursor.close()
	
	def get_ddl(self, date):
		self.TABLES["node_tbl"] = (
			"CREATE TABLE `"+date+"_node_tbl` ("
			"  `id` int NOT NULL,"
			"  `addr` varchar(16) NOT NULL,"
			"  `child` text,"
			"  `monitor` text,"
			"  PRIMARY KEY (`id`)"
			") ENGINE=InnoDB")

		self.TABLES["edge_tbl"] = (
			"CREATE TABLE `"+date+"_edge_tbl` ("
			"  `src` int NOT NULL,"
			"  `dst` int NOT NULL,"
			"  `rtt` text,"
			"  PRIMARY KEY (`src`, `dst`)"
			") ENGINE=InnoDB")
		
		self.TABLES["border_tbl"] = (
			"CREATE TABLE `"+date+"_border_tbl` ("
			"  `id` int NOT NULL,"
			"  `foreign_neighbours` text,"
			"  PRIMARY KEY (`id`)"
			") ENGINE=InnoDB")
		
		self.TABLES["geoip_tbl"] = (
			"CREATE TABLE `"+date+"_geoip_tbl` ("
			"  `id` int NOT NULL,"
			"  `geoip` text,"
			"  PRIMARY KEY (`id`)"
			") ENGINE=InnoDB")
	
	def drop_tbl(self, date):
		cursor = self.connect.cursor()
		tbl_suffix = ["_node_tbl","_edge_tbl","_border_tbl","_geoip_tbl"]
		for suf in tbl_suffix:
			ddl = "DROP TABLE IF EXISTS `"+date+suf+"`;"
			print ddl
			cursor.execute(ddl)
		
	def create_tbl(self, date):
		for name,ddl in self.TABLES.iteritems():
			cursor = self.connect.cursor()
			try:
				print "creating table {}:".format(date+"_"+name),
				print ddl,
				cursor.execute(ddl)
			except connector.Error as err:
				if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
					print("already exists.")
				else:
					print(err.msg)
			else:
				print ("OK")
			
			cursor.close()
	
	def export_graph_tbls(self, topo, date):
		#inserting into table: node_tbl
		node_tbl_name = date+"_node_tbl"
		print "inserting into: "+node_tbl_name
		cursor = self.connect.cursor()
		insert_node =	("INSERT INTO "+node_tbl_name+" "
				"(id, addr, child, monitor) "
				"VALUES (%s, %s, %s, %s)")
		for i in range(len(topo.node)):
			n = topo.node[i]
			id = i
			addr = n.addr
			child_str = ""
			for c in n.child:
				child_str = child_str+str(c)+"|"
			child_str = child_str.rstrip("|")
			
			monitor_str = ""
			for m in n.monitor:
				monitor_str = monitor_str+m+"|"
			monitor_str = monitor_str.rstrip("|")

			data_node = (id, addr, child_str, monitor_str)
			cursor.execute(insert_node, data_node)
		
		self.connect.commit()
		cursor.close()

		print "finished inserting into: "+node_tbl_name
		
		#inserting into table: edge_tbl
		edge_tbl_name = date+"_edge_tbl"
		print "inserting into: "+edge_tbl_name
		cursor = self.connect.cursor()
		insert_edge =	("INSERT INTO "+edge_tbl_name+" "
				"(src,dst,rtt) "
				"value(%s,%s,%s)")
		for i in range(len(topo.node)):
			n = topo.node[i]
			for c in n.child:
				rtt_str = ""
				rtt_list = n.child_rtt[c]
				for r in rtt_list:
					rtt_str = rtt_str+r+"|"
				rtt_str = rtt_str.rstrip("|")
				data_edge = (i,c,rtt_str)
				cursor.execute(insert_edge, data_edge)
			
		self.connect.commit()
		cursor.close()

		print "finished inserting into: "+edge_tbl_name
	
	def export_border_tbls(self, topo, date):
		#inserting into table: edge_tbl
		border_tbl_name = date+"_border_tbl"
		print "inserting into: "+border_tbl_name
		cursor = self.connect.cursor()
		insert_border =	("INSERT INTO "+border_tbl_name+" "
				"(id, foreign_neighbours) "
				"value(%s,%s)")
		for i in range(len(topo.node)):
			n = topo.node[i]
			if len(n.foreign_neighbours) == 0:
				continue
			nbr_str = ""
			for nbr in n.foreign_neighbours:
				nbr_str = nbr_str+str(nbr)+"|"
			nbr_str = nbr_str.rstrip("|")
			data_border = (i, nbr_str)
			cursor.execute(insert_border, data_border)
			
		self.connect.commit()
		cursor.close()

		print "finished inserting into: "+border_tbl_name

		#inserting into table: geoip_tbl
		geoip_tbl_name = date+"_geoip_tbl"
		print "inserting into: "+geoip_tbl_name
		cursor = self.connect.cursor()
		insert_geoip =	("INSERT INTO "+geoip_tbl_name+" "
				"(id, geoip) "
				"value(%s,%s)")
		for i in range(len(topo.node)):
			n = topo.node[i]
			geoip_str = json.dumps(n.geoip)
			data_geoip = (i, geoip_str)
			cursor.execute(insert_geoip, data_geoip)
			
		self.connect.commit()
		cursor.close()

		print "finished inserting into: "+geoip_tbl_name
