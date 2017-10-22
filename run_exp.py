import os
import sys
import subprocess
import time
import script
import tarfile
import shutil
import csv
import socket


from exp_setup import *

storm_exe = os.environ['STORM']
hostname = os.environ['HOSTNAME']

def kill_topologies(path):
	cmd = path+"/kill_topos.sh"
	#cmd = storm_exe + " kill "
	process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()


def clean_dirs_on_pis(pi_outdir):
	cmd = "dsh -aM -c rm " + pi_outdir + "/*"
	process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()

	if error is None:
		print "Delected output directories on PIs"


def conv_cap_to_dict(str):
	dict_ = {}
	str_ = str[1:-1]
	str_=str_.split(",")
	for s in str_:
		_ = s.split("=")
		dict_[_[0].strip()]=_[1].strip()
	return dict_

def get_topo_capacity_at_completion(in_topo_name, num_experiments, port=38999):
	host=''
	# create a socket 
    	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET-> IPv4, SOCK_STREAM-> TCP
   	# Bind socket to local host and port
    	try:
        	s.bind((host, port))
    	except socket.error as msg:
    		print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    		sys.exit()
	#Start listening on socket
	s.listen(10)
	print 'Listening for topology completion'
	
	completed=0
	topo_capacity_map={}
	while completed < num_experiments:
		#wait to accept a connection - blocking call
		conn, addr = s.accept()  # addr[0] -> IP, addr[1] -> PORT
		# Only reaceive the data once
		data=conn.recv(1024)
		comp=data.split("-")
		in_r=comp[0]
		cap_=conv_cap_to_dict(comp[1])
		topo_capacity_map[in_topo_name+str(in_r)] = cap_
		completed = completed+1
	s.close()
	return topo_capacity_map

def is_valid_topology(in_topo_name):
	"""
	returns True if the topology is a valid topology
	"""
	return in_topo_name in valid_topos


def run_tuning_experiment(topology, input_rate, num_events):
	# 1. Launch the topology
	# 2. Get capacity values for the topology
	# 3. Change the topology
	# 4. Repeat until max(capacities) >= 0.2
	pass

def launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events):
	executed_topologies = []
	commands = []

	for i in range(len(input_rates)):
		jar_path  = os.getcwd() + "/" + jar_name
		topo_unique_name = in_topo_name + "_" + str(input_rates[i])

		executed_topologies.append(topo_unique_name)

		command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
		commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]))
		
		print "Running experiment:"
		print commands[i] + "\n"
		process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
		output, error = process.communicate()
		#print output
		
	return (commands, executed_topologies)


def run_experiments(jar_name, in_topo_name, csv_file_name, duration):

	if in_topo_name not in valid_topos:
		print "not a valid topology name."
		print "can only execute " + str (valid_topos)
		exit(-1)

	input_rates = input_rates_dict[in_topo_name]
	num_events = num_events_dict[in_topo_name]

	data_file = data_files[in_topo_name]
	topo = topo_qualified_path[in_topo_name]
	prop_file = property_files[in_topo_name]

	exp_result_dir = results_dir[hostname]
	path = paths[hostname]

	os.chdir(path)

	# kill any topologies if running already
	kill_topologies(path)

	# Clean output directories on PIs
	clean_dirs_on_pis(pi_outdir)

	# Run the experiments now.
	commands = []
	executed_topologies=[]

	commands, executed_topologies = 
	for i in range(len(input_rates)):
		jar_path  = os.getcwd() + "/" + jar_name
		topo_unique_name = in_topo_name + "_" + str(input_rates[i])

		executed_topologies.append(topo_unique_name)

		command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
		commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]))
		
		print "Running experiment:"
		print commands[i] + "\n"
		process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
		output, error = process.communicate()
		#print output

	spout_files = []
	sink_files = []
	for i in range(len(executed_topologies)):
		spout_files.append(pi_outdir + "/" + "spout-" + executed_topologies[i] + "-1-1.0.log")
		sink_files.append(pi_outdir + "/" + "sink-" + executed_topologies[i] + "-1-1.0.log")



	# Experiments started....
	# Wait until they're done on the PIs (~ 12 minutes)

    # create a socket 
   # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET-> IPv4, SOCK_STREAM-> TCP
    # Bind socket to local host and port
    #try:
    #    s.bind((HOST, PORT))
    #except socket.error as msg:
    #	print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    #	sys.exit()
	#Start listening on socket
	#s.listen(10)
	#print 'Listening for topology completion'
	#
	#completed=0
	#comp_topologies=[]
	#while completed < num_experiments:
	#	#wait to accept a connection - blocking call
	#	conn, addr = s.accept()  # addr[0] -> IP, addr[1] -> PORT
	#	# Only reaceive the data once
	#	data=conn.recv(1024)
	#	comp=data.split("-")
	#	in_r=comp[0]
	#	cap_=conv_cap_to_dict(comp[1])
	#	print cap_
	#s.close()
	topo_cap_map = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=num_experiments)
	print (topo_cap_map)

	#start = time.time()
	#time.sleep(duration * 60) 
	#end = time.time()
	#print "waited for: " + str(end-start) + " secs"

	# Check which experiment was run on which PI device...
	devices = []
	for i in range(len(spout_files)):
		cmd = "dsh -aM -c ls " + spout_files[i]
		print cmd + "\n"
		process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
		output, error = process.communicate()
		devices.append(output.split(":")[0])

	if not os.path.isdir(exp_result_dir):
		os.mkdir(exp_result_dir)


	# Copy result files from the R.PIs to this machine
	for i in range(len(devices)):
		process = subprocess.Popen(["scp", devices[i] + ":" + spout_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
		output, error = process.communicate()
		# check for errors/output
		process = subprocess.Popen(["scp", devices[i] + ":" + sink_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
		output, error = process.communicate()
		# check for errors/outpu


	# output files have been copied to this machine
	# run script on these files to generate results...

	csv_file_name=path+"/"+csv_file_name
	shutil.copy(csv_file_name, exp_result_dir)
	os.chdir(exp_result_dir)
	with open (csv_file_name, "a") as csv_file:
		csv_file.write("\n\n")
		csv_file.write("experiment-"+jar_name+"-"+in_topo_name + "-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
		csv_file.write("\n")
		for i in range(num_experiments):
			spout_file = spout_files[i].split("/")[-1]
			sink_file = sink_files[i].split("/")[-1]
			resultObject = script.get_results(executed_topologies[i], spout_file, sink_file)
			results = resultObject.get_csv_rep(in_topo_name)
			if i == 0:
				csv_file.write("topology-in_rate," + results[0])
			csv_file.write("\n")
			csv_file.write(executed_topologies[i] + ",")
	        	csv_file.write(results[1])

	shutil.copy(csv_file_name, path) # move the modified result file back

	# get all the spout/sink logs and generated images and archive them.
	all_files = os.listdir(os.getcwd())
	tarfile_name = "experiment-"+jar_name+"-"+in_topo_name+ "-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()) + ".tar"
	out = tarfile.open(tarfile_name, mode='w')

	try:
		for _file in all_files:
			out.add(_file)
	finally:
		out.close()

	storm_exp_archive_dir = path+"/exp_archive_dir"

	if not os.path.isdir(storm_exp_archive_dir):
	        os.mkdir(storm_exp_archive_dir)
		
	shutil.copy(tarfile_name, storm_exp_archive_dir)

	# kill all the running topologies on PIs
	kill_topologies(path)

	# change directory back...
	os.chdir(path)
	shutil.rmtree(exp_result_dir)


def main():
    usage = "python <script_name.py> <jar_file> <topology_name> <csv_file_name> <topology_duration>"

    if len(sys.argv) != 5:
        print "Invalid number of arguments. See usage below."
        print usage
        exit(-1)

    jar_name = sys.argv[1]
    in_topo_name = sys.argv[2]
    csv_file_name = sys.argv[3]
    duration = float(sys.argv[4])

    run_experiments(jar_name, in_topo_name, csv_file_name, duration)


if __name__ == "__main__":
	main()
