import config
import os
import time
import signal

date = ""
date_dir = ""

def sig_handler(sig, frame):
	if(date != ""):
		print "analyze process for "+date+" terminated."
		os.system("rm -f "+date_dir+"/"+date+".log")
	exit()

def main():
	signal.signal(signal.SIGINT, sig_handler)

	cfg = config.get_config_section_dict("config.ini","data")
	log_dir = cfg["log_dir"]
	data_dir = cfg["data_dir"]
	error_dir = cfg["error_dir"]
	code_dir = cfg["code_dir"]

	error_file = error_dir+"/"+"error.log"
	
	fp = open(log_dir, 'rb')
	if (not os.path.exists(error_dir)):
		os.makedirs(error_dir)
	fe = open(error_file, 'a')
	for line in fp.readlines():
		if line == "":
			continue
		state = line.split(',')[0].split(' ')[4]
		if (state == "finished"):
			date = line.split(',')[0].split(' ')[3]
			date_dir = data_dir+"/"+date
			if (not os.path.exists(date_dir)):
				fe.write("directory: "+date_dir+" not found\n")
				continue
			#if (os.path.exists(date_dir+"/"+date+".log")):
			#	continue
			fork_id_1 = os.fork()
			if fork_id_1 > 0:
				os.waitpid(fork_id_1, 0)
				fork_id_2 = os.fork()
				if fork_id_2 > 0:
					os.waitpid(fork_id_2, 0)
				
				elif fork_id_2 == 0:
					print ("python analyze.py "+date+" app >> "+date_dir+"/"+date+".log")
					os.system("python analyze.py "+date+" app >> "+date_dir+"/"+date+".log")
					exit()

			
			elif fork_id_1 == 0:
				print ("python analyze.py "+date+" graph > "+date_dir+"/"+date+".log")
				os.system("python analyze.py "+date+" graph > "+date_dir+"/"+date+".log")
				exit()
	fp.close()
	fe.close()

if __name__ == "__main__":
	main()
