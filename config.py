import ConfigParser

def get_config_section_dict(config_file, header):
	parser = ConfigParser.ConfigParser();
	parser.read(config_file);
	config = {};
	map( lambda x:config.setdefault(x[0], x[1]), parser.items(header) );

	return config;
