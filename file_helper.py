import ip_topo
import json

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
