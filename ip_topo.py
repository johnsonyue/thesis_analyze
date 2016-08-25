import networkx as nx
import re
import geoip

class node:
	def __init__(self,ip):
		#basic info.
		self.addr = ip
		
		self.child = []
		self.child_rtt = {}
		
		#geoip info.
		self.geoip = {}
		
class topo_graph:
	def __init__(self):
		##basic adj list.
		#nodes.
		self.node = []
		#dict for quick node lookup.
		self.node_dict = {}
		
		##add root node.
		r = node("0.0.0.0")
		self.node.append(r)
		self.node_dict["0.0.0.0"] = 0
		
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
		self.geoip_helper = None
			
		##geoip info members.
		
		
	####
	##util functions.
	####
	def set_root_ip(self, root_ip):
		self.node[0].addr = root_ip
	
	def clear_visited_flags(self):
		for i in range(len(self.node)):
			self.is_node_visited[i] = False
	
	def get_node_num(self):
		return len(self.node)

	####
	##helper function for build method.
	####
	def parse_trace_caida(self, trace):
		for i in range(len(trace)):
			hop = []
			if trace[i] != "q":
				hop_sections = trace[i].split(';')[0]
				addr = hop_sections.split(',')[0]
				rtt = hop_sections.split(',')[1]
				hop = [addr, rtt]
			self.parse_hop(hop)
	
	#each hop contains a tuple of ip,rtt,nTries.
	def parse_hop(self, hop):
		if len(hop) == 0:
			return
		addr = hop[0]
		rtt = hop[1]
		
		#build graph from a trace.
		if not self.node_dict.has_key(addr):
			self.node.append(node(addr))
			cur_index = len(self.node)-1
			self.node_dict[addr] = cur_index

			self.prev_index = cur_index
		else:
			cur_index = self.node_dict[addr]
			self.prev_index = cur_index
		
		if self.prev_index != -1:
			prev_node = self.node[self.prev_index]
			if not prev_node.child_rtt.has_key(cur_index):
				prev_node.child.append(cur_index)
				prev_node.child_rtt[cur_index] = [rtt]
			else:
				prev_node.child_rtt[cur_index].append(rtt)
			
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

		print "building networkx graph object..."
		self.build_networkx_graph()
			
	def build_networkx_graph(self):
		for i in range(len(self.node)):
			for j in range(len(self.node[i].child)):
				self.networkx_graph.add_edge(i,self.node[i].child[j])
						
	def calc_networkx_lcc(self):
		print "getting the largest connected component..."
		self.networkx_lcc = max(nx.connected_component_subgraphs(self.networkx_graph), key = len)
	####
	##merge another graph into current graph.
	####
	def merge(self, topo):
		topo.clear_visited_flags()
		for i in range(len(topo.node)):
			if not topo.is_node_visited[i]:
				self.add_node(topo, i)
	
	def add_node(self, topo, ind):
		index = -1
		
		topo.is_node_visited[ind] = True
		n = topo.node[ind]
		#recursively get child.
		child = []
		for c in n.child:
			if not topo.is_node_visited[c]:
				ret = self.add_node(topo, c, topo.node[c])
				child.append(ret)

		#set index to return.
		#append or update node.
		if not self.node_dict.has_key(n.addr):
			self.node.append(n)
			index = len(self.node) - 1
			self.node_dict[n.addr] = index

			n.child = child
			for j in range(len(n.child)):
				self.networkx_graph.add_edge(index,n.child[j])
		else:
			index = self.node_dict[n.addr]
			for j in range(len(n.child)):
				is_included = False
				c = n.child[j]
				for ch in self.node[index].child:
					if c == ch:
						is_included = True
						break
				if not is_included:
					self.node[index].child.append(c)
					self.networkx_graph.add_edge(index,c)

		return index
	
	####
	##mark graph nodes.
	####
	def init_geoip(self):
		self.geoip_helper = geoip.geoip_helper()

	def mark_ip_geo(self):
		
	
		
