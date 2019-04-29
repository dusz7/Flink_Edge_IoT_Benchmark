import os
import sys
import argparse
import subprocess
import time
import socket
import tarfile
import shutil
import fileinput

from exp_setup import *

storm_exe = os.environ['STORM']
hostname = "buzz"
FNULL = open(os.devnull, 'w')

usage = "python <script_name.py> <jar_file> <topology_name>"
parser = argparse.ArgumentParser(description='run_exp script to run storm experiments')

parser.add_argument("jar_name")
parser.add_argument("in_topo_name")
parser.add_argument('--numWorkers', type=int, default=1, choices=xrange(1, 10), help="Nummber of workers to launch for the topology")

args = parser.parse_args()

def get_results(pi_outdir,  exp_result_dir):
    #devices = ["pi@raspberrypi1", "pi@raspberrypi2", "pi@raspberrypi3", "pi@raspberrypi11", "pi@raspberrypi12","pi@raspberrypi4", "pi@raspberrypi5", "pi@raspberrypi6", "pi@raspberrypi7", "pi@raspberrypi8","pi@raspberrypi9","pi@raspberrypi10"]
    devices = ["pi@raspberrypi11", "pi@raspberrypi12", "pi@raspberrypi7", "pi@raspberrypi8","pi@raspberrypi9","pi@raspberrypi1"]
    for dev in devices:
        process = subprocess.Popen(["scp", dev + ":" + pi_outdir + "/*", exp_result_dir+"/"], stdout=subprocess.PIPE)
        output, error = process.communicate()

def wait_for_completion(in_topo_name, num_experiments, start_time, port=38999):
    host = ''
    # create a socket 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET-> IPv4, SOCK_STREAM-> TCP

    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind socket to local host and port
    try:
        s.bind((host, port))
    except socket.error as msg:
        print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
    #Start listening on socket
    s.listen(10)
    print ('Experiments launched = ' + str(num_experiments) + 
        ', Waiting for completion...')
    
    completed=0
    while completed < num_experiments:
        #wait to accept a connection - blocking call
        conn, addr = s.accept()  # addr[0] -> IP, addr[1] -> PORT
        # Only reaceive the data once
        input_rate = conn.recv(4096)
        print ("Topology " + in_topo_name + " with input rate " + 
            str(input_rate) + " complete ({:.4f} min) ...".format((time.time() - start_time)/60))
            
        completed = completed+1
    s.close()
    print "Completed!"

def launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, num_workers):
    executed_topologies = []
    commands = []

    for i in range(len(input_rates)):
        jar_path  = os.getcwd() + "/" + jar_name
        topo_unique_name = in_topo_name + "_" + str(input_rates[i])

        executed_topologies.append(topo_unique_name)
                
        command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
        
        commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]) + " " + str(num_workers))
        
        print "Running experiment:"
        print commands[i] + "\n"
        process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        #print output

    return (commands, executed_topologies)

def clean_dirs_on_pis(pi_outdir):
    cmd = "dsh -aM -c rm " + pi_outdir + "/*"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "Deleted output directories on PIs"

def kill_topologies(path):
    cmd = path+"/kill_topos.sh"
    #cmd = storm_exe + " kill "
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def archive_results(path, jar_name, in_topo_name, exp_result_dir):
    with open(path + "/results.csv", 'a') as fout:
        fin = fileinput.input(exp_result_dir + "/" + in_topo_name + "_topo_summary.csv")
        for line in fin:
            fout.write(line)
        fin.close()
    
    os.chdir(exp_result_dir)
    # Remove logs
    # for f in glob.glob("*.log"):
    #    os.remove(f)

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
    
    os.chdir(path)
    shutil.rmtree(exp_result_dir)
    
    # kill all the running topologies on PIs
    kill_topologies(path)
    print "kill_topologies Completed!"

def run_experiments(jar_name, in_topo_name, numWorkers):
    input_rates = input_rates_dict[in_topo_name]
    num_events = num_events_dict[in_topo_name]
    
    data_file = data_files[in_topo_name]
    prop_file = property_files[in_topo_name]
    
    topo = topo_qualified_path[in_topo_name]
    
    exp_result_dir = results_dir[hostname]
    path = paths[hostname]
    
    os.chdir(path)
    
    # kill any topologies if running already
    kill_topologies(path)

    # Clean output directories on PIs
    clean_dirs_on_pis(pi_outdir)
    
    # Run the experiments now.
    commands, executed_topologies = launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, numWorkers)
    
    # Wait for completion
    wait_for_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=len(input_rates), start_time=time.time())
    
    # directory to get resullts of the experiments
    if not os.path.isdir(exp_result_dir):
        os.mkdir(exp_result_dir)
    
    get_results(pi_outdir, exp_result_dir)
    print "get_results Completed!"

def parse_results(topo_name, jar_name):
    input_rates = input_rates_dict[topo_name]
    input_rates_str = ""
    for i in range(len(input_rates)):
        input_rates_str += str(input_rates[i])
        if i < len(input_rates) - 1:
            input_rates_str += ","
    
    
    exp_result_dir = results_dir[hostname]
    
    command = "java -jar ParseResults.jar "
    command += topo_name + " "
    command += str(input_rates_str) + " "
    command += exp_result_dir + " "
    command += jar_name
    print command
    
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
def main():
    if args.in_topo_name not in valid_topos:
        print "not a valid topology name."
        print "can only execute " + str (valid_topos)
        exit(-1)

    run_experiments(args.jar_name, args.in_topo_name, args.numWorkers)
    parse_results(args.in_topo_name, args.jar_name)
    
    archive_results(paths[hostname], args.jar_name, args.in_topo_name, results_dir[hostname])
    
if __name__ == "__main__":
    main()
