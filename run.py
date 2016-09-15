import config
import os
import signal

def main():
	config = config.get_config_section_dict("config.ini","data")
	log_dir = config["log_dir"]
	data_dir = config["data_dir"]
	
	fp = open(log_dir, 'rb')
	for line in fp.readlines():
		state = line.split(',').split(' ')[4]
		if (state == "finished"):
			date = line.split(',').split(' ')
			date_dir = data_dir+"/"+date
			if (os.path.exists(date_dir+"/"+date+".log")):
				continue
			print ("python analyze.py "+date+" graph > "+date+".log &2>1; python analyze.py "+date+" app >> "+date+".log &2>1")
			os.system("python analyze.py "+date+" graph > "+date+".log &2>1; python analyze.py "+date+" app >> "+date+".log &2>1")

if __name__ == "__main__":
	main()
