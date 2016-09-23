import config
import ip_topo
import file_helper
import db_helper
import sys
import os
import re
import multiprocessing

class DateAnalyze():
	def __init__(self, date, file_name=""):
		self.config = config.get_config_section_dict("config.ini","data")
		self.data_dir = self.config["data_dir"]
		self.date = date
		
		self.topo = ip_topo.topo_graph()
		self.db_helper = db_helper.db_helper()
	
	####
	##basic graph analysis
	####
	def analyze_date_caida(self):
		date_dir = self.data_dir+"/"+self.date
		if (not os.path.exists(date_dir) or not os.listdir(date_dir)):
			print ("path is empty or not existent")
			exit()

		for fn in os.listdir(date_dir):
			if (re.findall(".*\.gz$",fn)):
				print "analyzing "+fn+" ..."
				if (os.path.exists(date_dir+"/"+fn.rstrip(".gz"))):
					os.system("rm -f "+date_dir+"/"+fn.rstrip(".gz"))
				warts_name = date_dir+"/"+fn.strip(".gz")
				dump_name = date_dir+"/"+warts_name.split('.')[-2]
				monitor_name = warts_name.split('.')[-2]
				
				temp_topo = ip_topo.topo_graph()
				temp_topo.build_popen_pipeline(date_dir, fn)
						
				self.topo.merge(temp_topo, monitor_name)
				print fn+" analyzed"
				print self.topo.get_node_num()
				print self.topo.get_target_num()

		print "exporting topo to db"
		self.export_topo_to_db()
		print "finished exporting topo to db"
	
	def analyze_date_caida_pipeline(self):
		self.topo = ip_topo.topo_graph()
		self.topo.build_pipeline()
		print self.topo.get_node_num(), self.topo.get_target_num()

		print "exporting topo to db"
		self.export_topo_to_db()
		print "finished exporting topo to db"

	####
	##file and db operations
	####
	def export_topo_to_db(self):
		self.db_helper.setup(self.date)
		self.db_helper.drop_graph_tbl(self.date)
		self.db_helper.create_tbl(self.date)
		self.db_helper.export_graph_tbls(self.topo, self.date)
		self.db_helper.close()

	def export_geoip_to_db(self):
		self.db_helper.setup(self.date)
		self.db_helper.drop_app_tbl(self.date)
		self.db_helper.create_tbl(self.date)
		self.db_helper.export_geoip_tbls(self.topo, self.date)
		self.db_helper.close()

	####
	##update accumulative node table.
	####
	def update_acc_node(self):
		self.db_helper.setup(self.date)
		self.db_helper.touch_acc()

		print "updating acc node ... "
		self.db_helper.update_nodes_to_acc(self.topo.node,self.date)
		self.db_helper.close()

	####
	##get monitor info
	####
	def get_monitor(self):
		file_helper.get_caida_monitor(self.data_dir+"/caida_monitor.json")
	
	####
	##geoip analysis
	####
	def analyze_geoip(self):
		date_dir = self.data_dir+"/"+self.date
		self.topo.init_geoip()
		self.topo.mark_geoip()
		#self.topo.get_foreign_neighbours()
		
		print "exporting geoip to db"
		self.export_geoip_to_db()
		print "finished exporting geoip to db"
	
def usage():
	print "usage: python analyze.py <date> <type>"
	print "e.g.:  python analyze.py 20160712 graph/app"

def main(argv):
	if (len(argv) < 3):
		usage()
		exit()

	date = argv[1]
	type = argv[2]
	analyze = DateAnalyze(date)
	if type == "date":
		analyze.analyze_date_caida_pipeline()
		analyze.analyze_geoip()
		analyze.update_acc_node()
	elif type == "monitor":
		print "getting monitor info ... "
		analyze.get_monitor()
		print "finished getting monitor info"

if __name__ == "__main__":
	main(sys.argv)
