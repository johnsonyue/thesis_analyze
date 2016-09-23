import networkx as nx
import re
import geoip
import os

class node:
	def __init__(self,ip):
		#basic info.
		self.addr = ip
		
		self.child = []
		self.child_rtt = {}
		
		#geoip info.
		self.geoip = {}
		self.foreign_neighbours = []
		
		#monitor info.
		self.monitor = []
		
class topo_graph:
	def __init__(self):
		##basic adj list.
		#nodes.
		self.node = []
		#dict for quick node lookup.
		self.node_dict = {}
		
		##adaption of networkx.
		#networkx topo graph.
		self.networkx_graph = nx.Graph()
		#largest connected component.
		self.networkx_lcc = None
	
		##helper global member.
		#helper memeber for build method.
		self.prev_index = 0
		#helper member for add_node method.
		self.is_node_visited = {}
			
		##geoip info members.
		self.geoip_helper = None
		
		##target.
		self.target_dict = {}
	
	####
	##util functions.
	####
	def get_node_num(self):
		return len(self.node)

	def get_target_num(self):
		return len(self.target_dict.keys())

	def is_reserved(self, addr):
		addr_list = addr.split('.')
		if addr_list[0] == "10":
			return True
		elif addr_list[0]+"."+addr_list[1] == "192.168":
			return True
		elif addr_list[0] == "172" and int(addr_list[1]) in range(16,32):
			return True
		else:
			return False

	####
	##build function
	####
	def parse_trace_caida(self, trace):
		for i in range(len(trace)):
			hop = []
			if trace[i] != "q":
				hop_sections = trace[i].split(';')[0]
				addr = hop_sections.split(',')[0]
				rtt = hop_sections.split(',')[1]
				hop = [addr, rtt]
				if(self.is_reserved(hop[0])):
					continue
			self.parse_hop(hop)
	
	#each hop contains a tuple of ip,rtt,nTries.
	def parse_hop(self, hop):
		if len(hop) == 0:
			self.prev_index = -1
			return
		addr = hop[0]
		rtt = hop[1]
		
		#build graph from a trace.
		if not self.node_dict.has_key(addr):
			self.node.append(node(addr))
			cur_index = len(self.node)-1
			self.node_dict[addr] = cur_index
		else:
			cur_index = self.node_dict[addr]
		
		if self.prev_index != -1:
			prev_node = self.node[self.prev_index]
			if not prev_node.child_rtt.has_key(cur_index):
				prev_node.child.append(cur_index)
				prev_node.child_rtt[cur_index] = [rtt]
			else:
				prev_node.child_rtt[cur_index].append(rtt)
		
		self.prev_index = cur_index
			
	#build graph from single node data.
	def build(self,file_name):
		print "parsing traces..."
		f = open(file_name, 'rb')
		for line in f.readlines():
			trace = []
			if (re.findall("#",line)):
				continue
			sections = line.strip('\n').split('\t')
			trace.extend(sections[13:])
			self.prev_index = 0
			self.parse_trace_caida(trace)

			#to record the destination of each trace.
			self.target_dict[sections[2]] = ""

	def build_popen_pipeline(self, date_dir, fn):
		print "parsing traces..."
		#start producer process.
		fl = os.popen("gzip -c -d -k -q "+date_dir+"/"+fn+" | sc_analysis_dump ")
		for line in fl:
			trace = []
			if (re.findall("#",line)):
				continue
			sections = line.strip('\n').split('\t')
			trace.extend(sections[13:])
			self.prev_index = 0
			self.parse_trace_caida(trace)

			#to record the destination of each trace.
			self.target_dict[sections[2]] = ""
	
	def build_pipeline(self):
		print "parsing traces..."
		#start producer process.
		try:
			while True:
				line = raw_input()
				if (re.findall("#",line)):
					continue
				trace = []
				sections = line.strip('\n').split('\t')
				trace.extend(sections[13:])
				self.prev_index = 0
				self.parse_trace_caida(trace)
	
				#to record the destination of each trace.
				self.target_dict[sections[2]] = ""
		except:
			return

	def build_iplane(self,file_name):
		print "parsing traces (iplane)..."
		f = open(file_name, 'rb')
		f.close()
		return ""

	def build_networkx_graph(self):
		print "building networkx graph object..."
		for i in range(len(self.node)):
			for j in range(len(self.node[i].child)):
				self.networkx_graph.add_edge(i,self.node[i].child[j])
		
	####
	##merge function
	####
	def clear_visited_flags(self):
		for i in range(len(self.node)):
			self.is_node_visited[i] = False

	def merge(self, topo, monitor):
		topo.clear_visited_flags()
		for i in range(len(topo.node)):
			if not topo.is_node_visited[i]:
				self.add_node(topo, i, monitor)
		
		#to merge destination dict.
		for k in topo.target_dict.keys():
			self.target_dict[k] = ""
	
	def add_node(self, topo, ind, monitor):
		index = -1
		
		topo.is_node_visited[ind] = True
		n = topo.node[ind]
		#recursively get child.
		child = []
		child_rtt = {}
		for c in n.child:
			if not topo.is_node_visited[c]:
				ret = self.add_node(topo, c, monitor)
				child.append(ret)
				#fix note: don't forget to update the member child_rtt too
				child_rtt[ret] = n.child_rtt[c]

		#set index to return.
		#append or update node.
		if not self.node_dict.has_key(n.addr):
			self.node.append(n)
			index = len(self.node) - 1
			self.node_dict[n.addr] = index

			n.child = child
			n.child_rtt = child_rtt

			n.monitor = [monitor]
		else:
			index = self.node_dict[n.addr]
			#fix note: use the updated child index instead of the old ones
			for j in range(len(child)):
				is_included = False
				c = child[j]
				for ch in self.node[index].child:
					if c == ch:
						is_included = True
						break
				if not is_included:
					self.node[index].child.append(c)
					self.node[index].child_rtt[c] = child_rtt[c]
				else:
					self.node[index].child_rtt[c].extend(child_rtt[c])

			is_included = False
			for j in range(len(n.monitor)):
				m = n.monitor[j]
				if (monitor == m):
					is_included = True
					break
			if not is_included:
				self.node[index].monitor.append(monitor)

		return index
	
	####
	##largest connected component
	####
	def calc_networkx_lcc(self):
		print "getting the largest connected component..."
		#self.networkx_lcc = max(nx.connected_component_subgraphs(self.networkx_graph), key = len)
		lcc_nodes = list(nx.dfs_preorder_nodes(self.networkx_graph, 0))
		lcc_nodes_dict = {}
		map ( lambda x:lcc_nodes_dict.setdefault(x,""), lcc_nodes )
		self.networkx_lcc = nx.Graph()
		for e in self.networkx_graph.edges():
			if ( lcc_nodes_dict.has_key(e[0]) or lcc_nodes_dict.has_key(e[1]) ):
				self.networkx_lcc.add_edge(e[0], e[1])
		print len(self.networkx_lcc)

	####
	##mark graph nodes.
	####
	def init_geoip(self):
		print "initializing geoip helper ..."
		self.geoip_helper = geoip.geoip_helper()
		print "finished initializing geoip helper"

	def mark_geoip(self):
		if (self.geoip_helper == None):
			print "use init_geoip() first"
			return
		
		print "marking node geolocation ..."
		for n in self.node:
			n.geoip = self.geoip_helper.query(n.addr)
		print "finished marking node geolocation"
	
	def get_foreign_neighbours(self):
		print "getting foreign neighbours ..."
		data_src = ["bgp","mmdb"]
		for e in self.networkx_graph.edges():
			src = e[0]
			dst = e[1]
			res = []
			for ds in data_src:
				src_country = self.node[src].geoip[ds]["country"].lower()
				dst_country = self.node[dst].geoip[ds]["country"].lower()
				if (src_country == "*" or dst_country == "*"):
					continue
				if (src_country != dst_country):
					res.append(ds)

			if (res != []):
				self.node[src].foreign_neighbours.append(dst)
				self.node[dst].foreign_neighbours.append(src)
		
		print "finished getting foreign neighbours"
