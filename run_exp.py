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


#TODO: should not take pi_outdir as parameter
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

	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
		topo_capacity_map[in_topo_name+'_'+str(in_r)] = cap_
		completed = completed+1
	s.close()
	return topo_capacity_map


def is_valid_topology(in_topo_name):
	"""
	returns True if the topology is a valid topology
	"""
	return in_topo_name in valid_topos


def run_tuning_experiment(jar_name, in_topo_name, csv_file_name):
	# 1. Launch the topology
	# 2. Get capacity values for the topology
	# 3. Change the topology
	# 4. Repeat until max(capacities) >= 0.2
	from exp_setup import *
	# Some setup...
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

	bolt_indices = topology_bolts[in_topo_name]
	total_bolts = len(bolt_indices)

	#initial_bolt_instances = ("1,"*total_bolts)[:-1]
	initial_bolt_instances=[1 for _ in range(total_bolts)]
	initial_bolt_instances=[list(initial_bolt_instances) for _ in range(num_experiments)]
	ir2bi = dict(zip(input_rates, initial_bolt_instances))
	print (ir2bi)

	
	# Explore upto 10 topologies
	for i in range(4):
		if len(input_rates) == 0:
			# no more experiments to run
			break

		num_experiments=len(input_rates)
		print ("Running experiments for input rates = " + str(input_rates))
		commands, executed_topologies = launch_tuning_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, ir2bi)

		# get spout and sink files
		spout_files, sink_files = get_spout_sink_files(pi_outdir, executed_topologies)
		
		# Wait until the experiments are done on the PIs
		# get capacities for all the launched experiments
		topo_cap_map = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=len(input_rates))
		print (topo_cap_map)

		# Update the capacities
		for key in topo_cap_map:
			#print("input_rate : " + key)
			bolt_with_max_cap = max(topo_cap_map[key], key=topo_cap_map[key].get) # get name of the bolt that has maximum capacity
			max_capacity = topo_cap_map[key][bolt_with_max_cap]
			#print("bolt with max cap = " + bolt_with_max_cap)
			ir=int(key.split("_")[1]) # input rate,
			print ("Comparing Values " + str(max_capacity) + " - 0.2 < 0 => " + str(float(max_capacity) - float(0.2) < 0))
			if float(max_capacity) - float(0.2) < 0:
				#remove this input rate from consideration
				print("Removing " + str(ir) + " from input_rates" )
				ind=input_rates.index(ir)
				input_rates.remove(ir)
				num_events.pop(ind)
			#print(ir2bi[ir])
			bolt_index_with_max_capacity = bolt_indices[bolt_with_max_cap]
			#print("index of bolt wiht max capacity = " + str(bolt_index_with_max_capacity))
			ir2bi[ir][bolt_index_with_max_capacity] = ir2bi[ir][bolt_index_with_max_capacity] + 1	
			#print(ir2bi[ir])

		print (ir2bi)

		# Check which experiment was run on which PI device...
		devices = get_devices_for_experiments(spout_files)

		# directory to get resullts of the experiments
		if not os.path.isdir(exp_result_dir):
			os.mkdir(exp_result_dir)

		# Copy result files from the R.PIs to this machine
		get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir)

		# output files have been copied to this machine
		# run script on these files to generate results...
		# After this, the results have been logged to the csv file
		get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_experiments, spout_files, sink_files, executed_topologies)

		# get all the spout/sink logs and generated images and archive them.
		archive_results(path, jar_name, in_topo_name)

		# kill all the running topologies on PIs
		kill_topologies(path)

		# change directory back...
		os.chdir(path)
		shutil.rmtree(exp_result_dir)




def launch_tuning_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, bolt_instances):
	"""
	bolt_instances : a dictionary that maps from input rate to a list of bolt instances where each list is specifies the number of instances for a particular topology
	"""
	executed_topologies = []
	commands = []

	for i in range(len(input_rates)):
		# get bolt_instances for topo with this input_rate
		bolt_instances_for_ir = bolt_instances[input_rates[i]]
		# convert bolt_instances to str 
		bolt_instances_for_ir = str(bolt_instances_for_ir)[1:-1].replace(" ", "")

		jar_path  = os.getcwd() + "/" + jar_name
		topo_unique_name = in_topo_name + "_" + str(input_rates[i])

		executed_topologies.append(topo_unique_name)

		command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
		commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]) + " " + bolt_instances_for_ir)
		
		print "Running experiment:"
		print commands[i] + "\n"
		process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
		output, error = process.communicate()
		#print output

	return (commands, executed_topologies)


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


def get_spout_sink_files(pi_outdir, executed_topologies):
	spout_files = []
	sink_files = []

	for i in range(len(executed_topologies)):
		spout_files.append(pi_outdir + "/" + "spout-" + executed_topologies[i] + "-1-1.0.log")
		sink_files.append(pi_outdir + "/" + "sink-" + executed_topologies[i] + "-1-1.0.log")

	return  (spout_files, sink_files)

def get_devices_for_experiments(spout_files):
	"""
	Checks where the Nimbus actually launched the experiments
	and returns the device for each experiment
	"""
	devices = []
	for i in range(len(spout_files)):
		cmd = "dsh -aM -c ls " + spout_files[i]
		#print cmd + "\n"
		process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
		output, error = process.communicate()
		devices.append(output.split(":")[0])
	return devices

def get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir):
	for i in range(len(devices)):
		process = subprocess.Popen(["scp", devices[i] + ":" + spout_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
		output, error = process.communicate()
		# check for errors/output
		process = subprocess.Popen(["scp", devices[i] + ":" + sink_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
		output, error = process.communicate()

def get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_experiments, spout_files, sink_files, executed_topologies):
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

def archive_results(path, jar_name, in_topo_name):
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


def run_experiments(jar_name, in_topo_name, csv_file_name):

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
	commands, executed_topologies = launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events)
	# Experiments have been started now

	# get spout and sink files
	spout_files, sink_files = get_spout_sink_files(pi_outdir, executed_topologies)
		
	# Wait until the experiments are done on the PIs
	# get capacities for all the launched experiments
	topo_cap_map = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=num_experiments)
	print (topo_cap_map)

	#start = time.time()
	#time.sleep(duration * 60) 
	#end = time.time()
	#print "waited for: " + str(end-start) + " secs"

	# Check which experiment was run on which PI device...
	devices = get_devices_for_experiments(spout_files)

	# directory to get resullts of the experiments
	if not os.path.isdir(exp_result_dir):
		os.mkdir(exp_result_dir)

	# Copy result files from the R.PIs to this machine
	get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir)

	# output files have been copied to this machine
	# run script on these files to generate results...
	# After this, the results have been logged to the csv file
	get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_experiments, spout_files, sink_files, executed_topologies)

	# get all the spout/sink logs and generated images and archive them.
	archive_results(path, jar_name, in_topo_name)

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
    tuning = sys.argv[4]
    if tuning == 'no':
    	run_experiments(jar_name, in_topo_name, csv_file_name)
    else:
    	run_tuning_experiment(jar_name, in_topo_name, csv_file_name)


if __name__ == "__main__":
	main()
