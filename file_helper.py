import ip_topo
import json
import pickle
import networkx as nx
import urllib2
import HTMLParser

#invoke after get_foreign_neighbours() method
def get_border_json(topo, file_name):
	border_list = []
	node_dict = {}
	for i in range(len(topo.node)):
		n = topo.node[i]
		if(len(n.foreign_neighbours) != 0):
			t = {i:n.foreign_neighbours}
			border_list.append(t)
			if (not node_dict.has_key(i)):
				node_dict[i] = {"address":n.addr, "geoip":n.geoip}
			for nbr in n.foreign_neighbours:
				if (not node_dict.has_key(nbr)):
					node_dict[i] = {"address":topo.node[nbr].addr, "geoip":topo.node[nbr].geoip}
	
	fp = open(file_name, 'wb')
	fp.write( json.dumps({"node":border_list, "geo":node_dict}) )
	fp.close()

#invoke after calc_networkx_lcc() method
def get_lcc_graph_json(topo, file_name):
	edges = topo.networkx_lcc.edges()
	nodes = {}
	for e in edges:
		for i in range(2):
			if(not nodes.has_key(e[i])):
				nodes[e[i]] = {"address" : topo.node[e[i]].addr}
		
	fp = open(file_name, 'wb')
	fp.write( json.dumps({"nodes":nodes,"edges":edges}) )
	fp.close()

def save_node(topo, file_name):
	fp = open(file_name, 'wb')
	pickle.dump(topo.node, fp, -1)
	fp.close()

def save_networkx_graph(topo, file_name):
	fp = open(file_name, 'wb')
	res = topo.networkx_graph.edges()
	pickle.dump(res, fp, -1)
	fp.close()

def restore_node(topo, file_name):
	fp = open(file_name, 'rb')
	topo.node = pickle.load(fp)
	fp.close()

def restore_networkx_graph(topo, file_name):
	fp = open(file_name, 'rb')
	edges = pickle.load(fp)

	topo.networkx_graph = nx.Graph()
	topo.networkx_graph.add_edges_from(edges)
	fp.close()

def get_caida_monitor():
	url = "http://www.caida.org/projects/ark/locations/"
	parser = MonitorParser()
	parser.feed()

#to get monitor information.
class MonitorParser(HTMLParser.HTMLParser):
	def __init__(self):
		HTMLParser.HTMLParser.__init__(self);
		self.is_monitor_table = False
		self.is_monitor_head = False
		self.is_monitor_data = False
		self.monitor_line = ""
		self.monitor_list = []

	def get_attr_value(self, target, attrs):
		for e in attrs:
			key = e[0];
			value = e[1];
			if (key == target):
				return value;
	
	def handle_starttag(self, tag, attrs):
		if (tag == "table" and self.get_attr_value("id", attrs) == "html_monitor_table"):
			self.is_monitor_table = True
		
		if (self.is_monitor_table and tag == "thead"):
			self.is_monitor_head = True
		if (self.is_monitor_table and tag == "td"):
			self.is_monitor_data = True
	
	def handle_data(self, data):
		if (self.is_monitor_data):
			self.monitor_line = self.monitor_line + "|" + data
		
	def handle_endtag(self, tag):
		if (tag == "table" and self.is_monitor_table):
			self.is_monitor_table = False
		if (tag == "thead" and self.is_monitor_table):
			self.is_monitor_head = False
		if (tag == "td" and self.is_monitor_data):
			self.is_monitor_data = False
		if (tag == "tr" and self.is_monitor_table and not self.is_monitor_head):
			line_list = self.monitor_line.split('|')
			line = ""
			for s in line_list:
				if not (s=="" or s==" " or s=="\n" or s=="Monitor" or s=="Data"):
					line = line + s + "|"
			self.monitor_list.append(line.rstrip("|"))
			self.monitor_line = ""

def get_caida_monitor(file_name):
	url = "http://www.caida.org/projects/ark/locations/"
	parser = MonitorParser()
	f = urllib2.urlopen(url)
	text = f.read()
	parser.feed(text)
		
	fp = open(file_name, 'wb')
	monitor_json = {}
	for m in parser.monitor_list:
		mon = {}
		mon_list = m.split("|")
		mon["activation"] = mon_list[1]
		mon["city"] = mon_list[2]
		mon["asn"] = mon_list[3]
		mon["organization"] = mon_list[4]
		monitor_json[mon_list[0]] = mon
	fp.write(json.dumps(monitor_json))
	fp.close()
