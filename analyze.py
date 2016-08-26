import config
import ip_topo
import file_helper
import sys
import os
import re

class DateAnalyze():
	def __init__(self, date):
		self.config = config.get_config_section_dict("config.ini","data")
		self.data_dir = self.config["data_dir"]
		self.date = date

		self.topo = ip_topo.topo_graph()
	
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
				os.system("sc_analysis_dump "+warts_name+" > "+dump_name)
				
				temp_topo = ip_topo.topo_graph()
				temp_topo.build(dump_name)
				self.topo.merge(temp_topo)
				
				os.system("rm -f "+warts_name)
				os.system("rm -f "+dump_name)
				print fn+" analyzed"
				print self.topo.get_node_num()
		
		self.topo.build_networkx_graph()
		self.topo.calc_networkx_lcc()

		file_helper.get_lcc_graph_json(self.topo, date_dir+"/"+self.date+".graph.json")

	def analyze_geoip(self):
		date_dir = self.data_dir+"/"+self.date
		self.topo.init_geoip()
		self.topo.mark_geoip()
		self.topo.get_foreign_neighbours()
		
		file_helper.get_border_json(self.topo, date_dir+"/"+self.date+".border.json")

def usage():
	print "usage: python analyze.py <date>"

def main(argv):
	if (len(argv) < 2):
		usage()
		exit()

	date = argv[1]
	analyze = DateAnalyze(date)
	analyze.analyze_date_caida()
	analyze.analyze_geoip()

if __name__ == "__main__":
	main(sys.argv)
