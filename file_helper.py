import ip_topo
import json
import pickle
import networkx as nx

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
				node_dict[i] = n.geoip
			for nbr in n.foreign_neighbours:
				if (not node_dict.has_key(nbr)):
					node_dict[i] = topo.node[nbr].geoip
	
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
	topo.networkx_graph.add_edge_from(edges)
	fp.close()
