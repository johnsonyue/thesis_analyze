import config
import ip_topo
import file_helper
import db_helper
import sys
import os
import re

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
				os.system("gzip -d -k -q "+date_dir+"/"+fn)
				warts_name = date_dir+"/"+fn.strip(".gz")
				dump_name = date_dir+"/"+warts_name.split('.')[-2]
				monitor_name = warts_name.split('.')[-2]
				os.system("sc_analysis_dump "+warts_name+" > "+dump_name)
				
				temp_topo = ip_topo.topo_graph()
				temp_topo.build(dump_name)
				self.topo.merge(temp_topo, monitor_name)
				
				os.system("rm -f "+warts_name)
				os.system("rm -f "+dump_name)
				print fn+" analyzed"
				print self.topo.get_node_num()
		
		self.topo.build_networkx_graph()
		
		print "pickling topo"
		self.pickle_topo()
		print "finished pickling topo"
		
		print "exporting topo to db"
		self.export_topo_to_db()
		print "finished exporting topo to db"

	####
	##file and db operations
	####
	def export_topo_to_db(self):
		self.db_helper.setup(self.date)
		self.db_helper.drop_tbl(self.date)
		self.db_helper.create_tbl(self.date)
		self.db_helper.export_graph_tbls(self.topo, self.date)
		self.db_helper.close()
	
	def export_border_to_db(self):
		self.db_helper.setup(self.date)
		self.db_helper.drop_tbl(self.date)
		self.db_helper.create_tbl(self.date)
		self.db_helper.export_border_tbls(self.topo, self.date)
		self.db_helper.close()
	
	def pickle_topo(self):
		date_dir = self.data_dir+"/"+self.date
		file_helper.save_node(self.topo, date_dir+"/"+self.date+".node.pkl")
		file_helper.save_networkx_graph(self.topo, date_dir+"/"+self.date+".networkx_graph.pkl")
	
	def restore_topo(self):
		date_dir = self.data_dir+"/"+self.date
		file_helper.restore_node(self.topo, date_dir+"/"+self.date+".node.pkl")
		file_helper.restore_networkx_graph(self.topo, date_dir+"/"+self.date+".networkx_graph.pkl")
	
	
	####
	##app analysis
	####
	def analyze_lcc(self):
		self.topo.calc_networkx_lcc()

		date_dir = self.data_dir+"/"+self.date

		#print "exporting networkx lcc json"
		#file_helper.get_lcc_graph_json(self.topo, date_dir+"/"+self.date+".lcc.json")
		#print "finished exporting networkx lcc json"
		

	def analyze_geoip(self):
		date_dir = self.data_dir+"/"+self.date
		self.topo.init_geoip()
		self.topo.mark_geoip()
		self.topo.get_foreign_neighbours()
		
		#file_helper.get_border_json(self.topo, date_dir+"/"+self.date+".border.json")
		print "exporting border(geoip) to db"
		self.export_border_to_db()
		print "finished exporting border(geoip) to db"


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
	if type == "graph":
		analyze.analyze_date_caida()
	elif type == "app":
		print "restoring topo"
		analyze.restore_topo()
		print "finished restoring topo"
		analyze.analyze_lcc()
		analyze.analyze_geoip()

if __name__ == "__main__":
	main(sys.argv)
