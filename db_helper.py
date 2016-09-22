import config
import mysql.connector as connector
from mysql.connector import errorcode
import json
import ip_topo
import re

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
	
	def setup_withoutdate(self):
		self.connect_to_db()
		self.use_database()
	
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
			"CREATE TABLE "+date+"_node_tbl ("
			"  id int NOT NULL,"
			"  addr varchar(16) NOT NULL,"
			"  child text,"
			"  monitor text,"
			"  PRIMARY KEY (id)"
			") ENGINE=InnoDB")

		self.TABLES["edge_tbl"] = (
			"CREATE TABLE "+date+"_edge_tbl ("
			"  src int NOT NULL,"
			"  dst int NOT NULL,"
			"  rtt text,"
			"  PRIMARY KEY (src, dst),"
			"  FOREIGN KEY (src) REFERENCES "+date+"_node_tbl(id),"
			"  FOREIGN KEY (dst) REFERENCES "+date+"_node_tbl(id)"
			") ENGINE=InnoDB")
		
		self.TABLES["border_tbl"] = (
			"CREATE TABLE "+date+"_border_tbl ("
			"  id int NOT NULL,"
			"  foreign_neighbours text,"
			"  PRIMARY KEY (id),"
			"  FOREIGN KEY (id) REFERENCES "+date+"_node_tbl(id)"
			") ENGINE=InnoDB")
		
		self.TABLES["geoip_tbl"] = (
			"CREATE TABLE "+date+"_geoip_tbl ("
			"  id int NOT NULL,"
			"  geoip text,"
			"  PRIMARY KEY (id),"
			"  FOREIGN KEY (id) REFERENCES "+date+"_node_tbl(id)"
			") ENGINE=InnoDB")
		
				
		self.TABLES["acc_tbl"] = (
			"CREATE TABLE IF NOT EXISTS acc_tbl ("
			"  addr varchar(16) NOT NULL,"
			"  geoip text,"
			"  log text,"
			"  PRIMARY KEY (addr),"
			"  INDEX ind_addr (addr)"
			")ENGINE=InnoDB")
		
	def drop_graph_tbl(self, date):
		#drop app table first because of the foreign key constraints.
		self.drop_app_tbl(date)
		cursor = self.connect.cursor()
		tbl_suffix = ["_edge_tbl","_node_tbl"]
		for suf in tbl_suffix:
			ddl = "DROP TABLE IF EXISTS "+date+suf+";"
			print ddl
			cursor.execute(ddl)
	
	def drop_app_tbl(self, date):
		cursor = self.connect.cursor()
		tbl_suffix = ["_border_tbl","_geoip_tbl"]
		for suf in tbl_suffix:
			ddl = "DROP TABLE IF EXISTS "+date+suf+";"
			print ddl
			cursor.execute(ddl)

	def create_tbl(self, date):
		tbl_list = ["node_tbl", "edge_tbl", "border_tbl", "geoip_tbl"]
		for name in tbl_list:
			ddl = self.TABLES[name]
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
	
	def export_geoip_tbls(self, topo, date):
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
	
	def restore_topo(self, topo, date):
		#restoring from table: node_tbl
		node_tbl_name = date+"_node_tbl"
		print "restoring from: "+node_tbl_name
		cursor = self.connect.cursor()
		select_node =	("SELECT id, addr, child, monitor "
				"FROM "+node_tbl_name+" "
				"ORDER BY id ASC")
		print select_node
		
		cursor.execute(select_node)
		for (id, addr, child, monitor) in cursor:
			n = ip_topo.node(addr)
			n.child = []
			if child != "":
				map ( lambda x: n.child.append(int(x)), child.split('|') )
			n.monitor = monitor.split('|')
			topo.node.append(n)
		cursor.close()

		print "finished restoring from: "+node_tbl_name
		
		#restoring from table: edge_tbl
		edge_tbl_name = date+"_edge_tbl"
		print "restoring from: "+edge_tbl_name
		cursor = self.connect.cursor()
		select_edge =	("SELECT src,dst,rtt "
				"FROM "+edge_tbl_name+" ")
		print select_edge

		cursor.execute(select_edge)
		for (src, dst, rtt) in cursor:
			rtt_list = rtt.split('|')
			topo.node[src].child_rtt[dst] = rtt_list
		cursor.close()

		print "finished selecting from: "+edge_tbl_name
		
		#rebuild networkx graph
		topo.build_networkx_graph()

	def get_info(self, date):
		node_cnt = -1
		node_tbl_name = date+"_node_tbl"
		cursor = self.connect.cursor()
		select_node = "SELECT count(*) as count FROM "+node_tbl_name
		print select_node
		
		cursor.execute(select_node)
		for (count) in cursor:
			node_cnt = count

		edge_cnt = -1
		edge_tbl_name = date+"_edge_tbl"
		select_node = "SELECT count(*) as count FROM "+edge_tbl_name
		print select_node
		cursor.execute(select_node)
		for (count) in cursor:
			edge_cnt = count

		cursor.close()
		
		return {"node_cnt":node_cnt, "edge_cnt":edge_cnt}
	
	def get_addr(self, date, ind):
		#restoring from table: node_tbl
		cursor = self.connect.cursor()
		select_node =   ("SELECT node.addr FROM "+date+"_node_tbl as node "
				"WHERE node.id="+str(ind))
		
		cursor.execute(select_node)
		res = ""
		for c in cursor:
			res = c[0]
		cursor.close()

		return res
	
	def get_edge_list(self, date, start, limit):
		#restoring from table: node_tbl
		cursor = self.connect.cursor(buffered=True)
		select_node = ("SELECT src,dst FROM "+date+"_edge_tbl LIMIT "+str(start)+","+str(limit))
		print select_node
		res = []
		cursor.execute(select_node)
		for (src,dst) in cursor:
			t = (self.get_addr(date,src), self.get_addr(date,dst))
			res.append(t)
			c = cursor.fetchone()
		cursor.close()

		return res
	
	def create_addr_index(self, date):
		cursor = self.connect.cursor()
		create_index =  ("CREATE INDEX "+date+"_ind_addr ON "+
				date+"_node_tbl (addr)")
		try:
			print "creating index {}:".format(date+"_node_tbl"),
			print create_index
			cursor.execute(create_index)
		except connector.Error as err:
			if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
				print("already exists.")
			else:
				print(err.msg)
		else:
			print ("OK")
		
		create_index =  ("CREATE INDEX "+date+"_ind_id ON "+
				date+"_node_tbl (id)")
		try:
			print "creating index {}:".format(date+"_node_tbl"),
			print create_index
			cursor.execute(create_index)
		except connector.Error as err:
			if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
				print("already exists.")
			else:
				print(err.msg)
		else:
			print ("OK")


		cursor.close()
	
	def get_geoip(self, date, addr):
		cursor = self.connect.cursor()
		select_node =   ("SELECT geoip.geoip FROM "+date+"_node_tbl as node, "+
				date+"_geoip_tbl as geoip WHERE node.addr=\""+addr+"\" and "
				"node.id=geoip.id")
		
		cursor.execute(select_node)
		res = ""
		for (geoip) in cursor:
			res = geoip
		return res

	def touch_acc(self):
		tbl_name = "acc_tbl"
		ddl = self.TABLES[tbl_name]
		cursor = self.connect.cursor()
		try:
			print "creating table {}:".format(tbl_name),
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
	
	def update_nodes_to_acc(self, nodes, date):
		cursor = self.connect.cursor()

		for nd in nodes:
			addr = nd.addr
			geoip = json.dumps(nd.geoip)
			acc_tbl_name = "acc_tbl"
			select_node =   ("SELECT addr,geoip,log FROM "+acc_tbl_name+" "
					"WHERE addr=\""+addr+"\"")
			
			cursor.execute(select_node)
	
			node = None
			for n in cursor:
				node = n
			if node:
				log = node[2]
				if (not re.findall(date, log)):
					log = log+"|"+date
					update_node =   ("UPDATE "+acc_tbl_name+" "
							"SET log="+log+" "
							"WHERE addr=\""+addr+"\"")
					cursor.execute(update_node)
			else:
				insert_node =   ("INSERT INTO "+acc_tbl_name+" "
						"VALUES (%s, %s, %s)")
				data_node = (addr, json.dumps(geoip), date)
				cursor.execute(insert_node, data_node)
			
		self.connect.commit()
		cursor.close()
