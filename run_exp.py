#################################################################################
#   Document here...
#################################################################################

import os
import sys
import subprocess
import time
import script
import tarfile
import shutil
import csv
import socket
import pandas as pd
import argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import re
from scipy.interpolate import spline

from exp_setup import *

storm_exe = os.environ['STORM']
# to get results_directory mapping for the machine running 
# the experiment the mapping is defined in exp_setup.py
hostname = os.environ['HOSTNAME'] 
bpMonitor = False


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
        if len(_) == 2:
            dict_[_[0].strip()] = float(_[1].strip("{} "))
    return dict_

def conv_to_dict(str):
    dict_ = {}
    str_ = str[1:-1]
    str_=str_.split(",")
    for s in str_:
        _ = s.split("=")
        if len(_) == 2:
            dict_[_[0].strip()] = _[1].strip("{} ")
    return dict_

_nsre = re.compile('([0-9]+)')
def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(_nsre, s)] 


def get_topo_capacity_at_completion(in_topo_name, num_experiments, port=38999):
    """
    Returns topo_capacity_map and topo_exec_lat_map
    topo_capacity_map is a mapping from topo+inputrate to a dictionary for bolts->capacities
    """
    host=''
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
        ', Waiting for experiments completion')
    
    completed=0
    topo_capacity_map={}
    topo_exec_lat_map={}
    component_to_worker_map={}
    while completed < num_experiments:
        #wait to accept a connection - blocking call
        conn, addr = s.accept()  # addr[0] -> IP, addr[1] -> PORT
        # Only reaceive the data once
        data=conn.recv(4096)
        comp=data.split("&")
        in_r=comp[0]
        print("Got result for experiment with topology: " + in_topo_name + 
            ", input rate = " + str(in_r))

        cap_=conv_cap_to_dict(comp[1])  #Get dictionaty for capacity values
        lat_=conv_cap_to_dict(comp[2])  #Get dictionary for latency values
        c2w_=conv_to_dict(comp[3])  #Get dictionary for component/bolt to worker mapping
        # print ("c2w_")
        # print (c2w_)

        topo_capacity_map[in_topo_name+'_'+str(in_r)] = cap_
        topo_exec_lat_map[in_topo_name+'_'+str(in_r)] = lat_
        component_to_worker_map[in_topo_name+"_"+str(in_r)] = c2w_

        completed = completed+1
    s.close()
    return (topo_capacity_map, topo_exec_lat_map, component_to_worker_map)

def is_valid_topology(in_topo_name):
    """
    returns True if the topology is a valid topology
    """
    return in_topo_name in valid_topos

def print_dict_of_dicts(_dict):
    """
    Formats a dict of dict as:
    key1
    key1_1,value1
    key1_2,value2
    #############
    key2
    key2_1,value1
    key2_2,value2
    """
    res=""
    for key in _dict:
        res+=key+"\n"
        new_dict = _dict[key]
        for _key in new_dict:
            res+=_key+","+new_dict[_key]+"\n"
        res+="\n-----------------------------n"

    return res

def format_dict(_dict, indicator):
    """
    Formats a dictionary as:-
        key1:value1
        key2:value2
        key3:value3
            OR
        key1
        value1
        key2
        value2
    based on the value of the indicator
    """
    res="\n\n"
    for key in _dict:
        if indicator:
            res+=str(key)+":"
        else:
            res+=str(key)+"\n"
        val = _dict[key]
        res+=str(val) + "\n"
    return res

def write_to_csv_file(path, _file_name, _str):
    """
    Writes the string _str to the file 
    "_file_name" in directory "path"
    """
    file_name = path+"/"+_file_name
    with open(file_name, 'a') as f:
        f.write(_str)


def launch_tuning_explore_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, bolt_instances, riot, num_workers):
    """
    bolt_instances : a dictionary that maps from input rate to a list of bolt 
    instances where each list is specifies the number of instances for a particular topology
    """
    executed_topologies = []
    commands = []
    bolt_instances = str(bolt_instances)[1:-1].replace(" ", "")
    print(bolt_instances)

    for i in range(len(input_rates)):
        # get bolt_instances for topo with this input_rate
        # bolt_instances_for_ir = bolt_instances[input_rates[i]]
        # # convert bolt_instances to str 
        # bolt_instances_for_ir = str(bolt_instances_for_ir)[1:-1].replace(" ", "")

        jar_path  = os.getcwd() + "/" + jar_name
        topo_unique_name = in_topo_name + "_" + str(input_rates[i])

        executed_topologies.append(topo_unique_name)
                
        if riot:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
        else:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " 1 " + pi_outdir
                    
        commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]) + " " + str(num_workers) + " " + bolt_instances)
        
        print "Running experiment:"
        print commands[i] + "\n"
        process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        #print output

    return (commands, executed_topologies)


def explore_with_input_rates(jar_name, in_topo_name, csv_file_name, riot, singleStep, numIterations, numWorkers, latencyThreshold):
    """
    Steps:
    1. Select [1,1,1,...1] as initial topology T
    2. Run experiments with all input rates for topology T
    3. Select capacities for input rate that gives Latency > ThreasholdLatency
    4. Increase bolt instances according to capacitites of that topology.
    5. Go back to step 2 and repeat.
    """
    exp_result_file_name = "tuning_exp_result.csv"
    input_rates = list(input_rates_dict[in_topo_name])
    num_events = list(num_events_dict[in_topo_name])
    num_exp = len(input_rates)
    num_launches = num_exp//num_pis if num_exp%num_pis == 0 else num_exp//num_pis + 1

    if riot:
        data_file = data_files[in_topo_name]
        prop_file = property_files[in_topo_name]
    else:
        data_file = None
        prop_file = None

    topo = topo_qualified_path[in_topo_name]

    exp_result_dir = results_dir[hostname]
    path = paths[hostname]
    os.chdir(path)

    kill_topologies(path)
    clean_dirs_on_pis(pi_outdir)

    bolt_indices = topology_bolts[in_topo_name]
    total_bolts = len(bolt_indices)

    # Initial Bolt instances - all set to 1
    initial_bolt_instances=[1 for _ in range(total_bolts)]
    # initial_bolt_instances=[list(initial_bolt_instances) for v in range(num_exp)]
    bolt_instances = initial_bolt_instances

    capacities_dfs = []
    latencies_dfs = []
    bolt_instances_dfs = []
    results_dfs = []
    
    latencies = [[None for _ in range(numIterations)] for _ in range(len(topo_to_paths[in_topo_name]))]
    throughputs = [[None for _ in range(numIterations)] for _ in range(len(topo_to_paths[in_topo_name]))]

    # Explore upto 10 topologies
    for i in range(numIterations):
        print ("Iteration = " + str(i))
        print ("Topology = " + str(bolt_instances))
        write_to_csv_file(path, exp_result_file_name, "\n\n################ Iteration = " + str(i) + " ###################\n")

        print ("Running experiments for input rates = " + str(input_rates))
        commands, executed_topologies = launch_tuning_explore_experiment(input_rates, in_topo_name, jar_name, 
                        topo, data_file, pi_outdir, prop_file, num_events, bolt_instances, riot, numWorkers)

        # get spout and sink files
        spout_files, sink_files, bp_files = get_spout_sink_files(pi_outdir, executed_topologies, riot)
        
        # Wait until the experiments are done on the PIs
        # get capacities for all the launched experiments
        (topo_cap_map, topo_lat_map, component_to_worker_map) = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=len(input_rates))

        #write_to_csv_file (path, "bolt_capacities.csv", print_dict_of_dicts(topo_cap_map)) # Log per-bolt capacities for all the experiments
        _file = path+"/"+exp_result_file_name
        write_to_csv_file(path, exp_result_file_name, "\nCapacities:\n")
        cap_df = pd.DataFrame(topo_cap_map)
        cap_df = cap_df.round(4)
        cap_df_var = cap_df.apply(lambda x: np.sum(np.square(x - np.mean(x))) / len(x), axis=0)
        cap_df_min= cap_df.apply(lambda x: min(x), axis=0)
        cap_df.loc['variance'] = cap_df_var
        cap_df.loc['minimum'] = cap_df_min
        capacities_dfs.append(cap_df)
        cap_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')

        #write_to_csv_file (path, "bolt_capacities.csv", print_dict_of_dicts(topo_lat_map)) # Log per-bolt avg. execution latency for all the experiments
        lat_df = pd.DataFrame(topo_lat_map)
        lat_df = lat_df.round(4)
        write_to_csv_file(path, exp_result_file_name, "\nLatencies:\n")
        latencies_dfs.append(lat_df)
        lat_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')
    
        spout_devices, sink_devices = get_spout_sink_devices(in_topo_name, input_rates, component_to_worker_map)

        # directory to get resullts of the experiments
        if not os.path.isdir(exp_result_dir):
            os.mkdir(exp_result_dir)

        # Copy result files from the R.PIs to this machine
        # get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir)
        get_spout_sink_log_files(spout_devices, sink_devices, spout_files, sink_files, bp_files, exp_result_dir)

        # output files have been copied to this machine
        # run script on these files to generate results...
        # After this, the results have been logged to the csv file
        _results = get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_exp, spout_files, sink_files, bp_files, executed_topologies)
        write_to_csv_file(path, exp_result_file_name, "\nMeasured Throughput and Latency:\n")
        results_df = pd.DataFrame(_results)
        for col in results_df.columns:
            results_df[col] = pd.to_numeric(results_df[col])
        results_df = results_df.round(2)
        
        print ("\nDataframe\n")
        print(results_df)
        results_df.to_pickle(path+"/results_df")

        print (cap_df)
        cap_df.to_pickle(path+'/cap_df')

        results_dfs.append(results_df)
        results_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')



        select_ir = (results_df.loc[latency_explore_paths[in_topo_name]] - 
            latencyThreshold).abs().sort_values().index[0]

        # select_ir = select_topo.split('_')[1]
        print ("\nSelected Input rate: " + str(select_ir)+"\n")
        
        topo_paths = topo_to_paths[in_topo_name]
        for _iter, _path in enumerate(topo_paths):
            t = "throughput_" + _path
            l = "latency_" + _path
            latencies[_iter][i] = results_df.loc[l][select_ir]
            throughputs[_iter][i] = results_df.loc[t][select_ir]

        # Update the capacities for each topology that was launched.
        capacities = topo_cap_map[select_ir]

        key = select_ir
        topo_cap_map[key].pop('sink', None) 
        if in_topo_name == 'pred':
            topo_cap_map[key].pop('LinearRegressionPredictorBolt', None) 
        
        bolt_with_max_cap = max(topo_cap_map[key], key=topo_cap_map[key].get) # get name of the bolt that has maximum capacity
        
        max_capacity = topo_cap_map[key][bolt_with_max_cap]
        #print("bolt with max cap = " + bolt_with_max_cap)
        ir=int(key.split("_")[1]) # input rate,

        print(str(bolt_with_max_cap) + ", " + str(max_capacity))

        bolt_index_with_max_capacity = bolt_indices[bolt_with_max_cap]
        bolt_instances[bolt_index_with_max_capacity] = bolt_instances[bolt_index_with_max_capacity] + 1

        # get all the spout/sink logs and generated images and archive them.
        archive_results(path, jar_name, in_topo_name)
        kill_topologies(path)
        # change directory back...
        os.chdir(path)
        shutil.rmtree(exp_result_dir)

    print (latencies)
    print (throughputs)

    if not os.path.isdir("output_images"):
                os.mkdir("output_images")

    for _iter in range(len(latencies)):
        _path = topo_to_paths[in_topo_name][_iter]
        fig, ax = plt.subplots()
        ax.plot(latencies[_iter])
        ax.set_xlabel('Iterations')
        ax.set_ylabel('Latency')
        ax.set_title("Latency " + _path)
        plt.savefig(path+"/output_images/" + "Latency_" + _path)

        fig, ax = plt.subplots()
        ax.plot(throughputs[_iter])
        ax.set_xlabel('Iterations')
        ax.set_ylabel('Throughput')
        ax.set_title("Throughput " + _path)
        plt.savefig(path+"/output_images/" + "Throughput_" + _path)


def run_tuning_experiment(jar_name, in_topo_name, csv_file_name, riot, singleStep, numIterations, numWorkers):
    # Runs the same topology with different input rates...
    # 1. Launch the topology
    # 2. Get capacity values for the topology
    # 3. Change the topology
    # 4. Repeat until max(capacities) >= 0.2

    num_exp = num_experiments

    exp_result_file_name = "tuning_exp_result.csv"

    input_rates = list(input_rates_dict[in_topo_name])
    num_events = list(num_events_dict[in_topo_name])
    prev_input_rates = []

    if riot:
        data_file = data_files[in_topo_name]
        prop_file = property_files[in_topo_name]
    else:
        data_file = None
        prop_file = None

    topo = topo_qualified_path[in_topo_name]

    exp_result_dir = results_dir[hostname]
    path = paths[hostname]
    os.chdir(path)

    # kill any topologies if running already
    kill_topologies(path)

    # Clean output directories on PIs
    clean_dirs_on_pis(pi_outdir)

    bolt_indices = topology_bolts[in_topo_name]
    total_bolts = len(bolt_indices)

    # Initial Bolt instances - all set to 1
    initial_bolt_instances=[1 for _ in range(total_bolts)]
    initial_bolt_instances=[list(initial_bolt_instances) for v in range(num_exp)]
    ir2bi = dict(zip(input_rates, initial_bolt_instances))
    print (ir2bi)

    ir2bis = {}
    for k in ir2bi.keys():
        ir2bis[k]=[[]]

    capacities_dfs = []
    latencies_dfs = []
    bolt_instances_dfs = []
    results_dfs = []
    

    # Explore upto 10 topologies
    for i in range(numIterations):
        prev_input_rates = list(input_rates)
        if len(input_rates) == 0:
            # no more experiments to run
            break

        write_to_csv_file(path, exp_result_file_name, "\n\n################################### Iteration = " + str(i) + " #######################################\n")

        num_exp=len(input_rates)
        print ("Running experiments for input rates = " + str(input_rates))
        commands, executed_topologies = launch_tuning_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, ir2bi, riot, numWorkers)

        # get spout and sink files
        spout_files, sink_files, bp_files = get_spout_sink_files(pi_outdir, executed_topologies, riot)
        
        # Wait until the experiments are done on the PIs
        # get capacities for all the launched experiments
        (topo_cap_map, topo_lat_map, component_to_worker_map) = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=len(input_rates))
        print ("component_to_worker_map")
        print (component_to_worker_map)

        #write_to_csv_file (path, "bolt_capacities.csv", print_dict_of_dicts(topo_cap_map)) # Log per-bolt capacities for all the experiments
        _file = path+"/"+exp_result_file_name
        write_to_csv_file(path, exp_result_file_name, "\nCapacities:\n")
        cap_df = pd.DataFrame(topo_cap_map)
        cap_df = cap_df.round(4)
        cap_df_var = cap_df.apply(lambda x: np.sum(np.square(x - np.mean(x))) / len(x), axis=0)
        cap_df_min= cap_df.apply(lambda x: min(x), axis=0)
        cap_df.loc['variance'] = cap_df_var
        cap_df.loc['minimum'] = cap_df_min
        capacities_dfs.append(cap_df)
        cap_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')

        #write_to_csv_file (path, "bolt_capacities.csv", print_dict_of_dicts(topo_lat_map)) # Log per-bolt avg. execution latency for all the experiments
        lat_df = pd.DataFrame(topo_lat_map)
        lat_df = lat_df.round(4)
        write_to_csv_file(path, exp_result_file_name, "\nLatencies:\n")
        latencies_dfs.append(lat_df)
        lat_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')

        #print(lat_df)

        # Update the capacities for each topology that was launched.
        for key in topo_cap_map:
            
            topo_cap_map[key].pop('sink', None) # Delete sink bolt's capacity from the dict. What if sink bolt has max capacity, Don't want to increase its bolts...
            if in_topo_name == 'pred':
                topo_cap_map[key].pop('LinearRegressionPredictorBolt', None) # LinRegPred bolt can't be parallelized. Don't increase its instances
            
            bolt_with_max_cap = max(topo_cap_map[key], key=topo_cap_map[key].get) # get name of the bolt that has maximum capacity
            

            max_capacity = topo_cap_map[key][bolt_with_max_cap]
            #print("bolt with max cap = " + bolt_with_max_cap)
            ir=int(key.split("_")[1]) # input rate,

            ir2bis[ir].append(list(ir2bi[ir]))

            print ("Comparing Values " + str(max_capacity) + " - 0.2 < 0 => " + str(float(max_capacity) - float(0.2) < 0))

            # If the capacity falls below a certain threshold, stop considering this input rate..
            if float(max_capacity) - float(0.2) < 0:
                #remove this input rate from consideration
                print("Removing " + str(ir) + " from input_rates" )
                ind=input_rates.index(ir)
                input_rates.remove(ir)
                num_events.pop(ind)
            
            # Bolt instance update policy
            if singleStep:
                min_cap_bolt = min(topo_cap_map[key], key=topo_cap_map[key].get) # get name of bolt with min capacity
                min_cap = topo_cap_map[key][min_cap_bolt]
                bolt_cap_dict=topo_cap_map[key]
                d = {}
                for k, v in bolt_cap_dict.items():
                    d[k] = int(float(v)//min_cap)
                for k,v in d.items():
                    if k in bolt_indices.keys():
                        ir2bi[ir][bolt_indices[k]] = v    
            else:
                bolt_index_with_max_capacity = bolt_indices[bolt_with_max_cap]
                ir2bi[ir][bolt_index_with_max_capacity] = ir2bi[ir][bolt_index_with_max_capacity] + 1

        print("ir2bis")
        print(ir2bis)

        write_to_csv_file(path, exp_result_file_name, "\n# Bolt Instances:\n")
        ir2bi_df = pd.DataFrame(ir2bi)
        bolt_instances_dfs.append(ir2bi_df)
        ir2bi_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')
        
        # Log the increment to the bolt instances as a result of observed capacities.
        #write_to_csv_file(path, exp_result_file_name, "\n" + format_dict(ir2bi, True) + "\n")  

        # Check which experiment was run on which PI device...
        # devices = get_devices_for_experiments(spout_files)
        spout_devices, sink_devices = get_spout_sink_devices(in_topo_name, prev_input_rates, component_to_worker_map)

        # directory to get resullts of the experiments
        if not os.path.isdir(exp_result_dir):
            os.mkdir(exp_result_dir)

        # Copy result files from the R.PIs to this machine
        # get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir)
        get_spout_sink_log_files(spout_devices, sink_devices, spout_files, sink_files, bp_files, exp_result_dir)

        # output files have been copied to this machine
        # run script on these files to generate results...
        # After this, the results have been logged to the csv file
        _results = get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_exp, spout_files, sink_files, bp_files, executed_topologies)
        write_to_csv_file(path, exp_result_file_name, "\nMeasured Throughput and Latency:\n")
        results_df = pd.DataFrame(_results)
        for col in results_df.columns:
            results_df[col] = pd.to_numeric(results_df[col])
        results_df = results_df.round(2)
        results_dfs.append(results_df)
        results_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')
        
        # get all the spout/sink logs and generated images and archive them.
        archive_results(path, jar_name, in_topo_name)

        # kill all the running topologies on PIs
        kill_topologies(path)

        # change directory back...
        os.chdir(path)
        shutil.rmtree(exp_result_dir)

    return ir2bis

def launch_tuning_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, bolt_instances, riot, num_workers):
    """
    bolt_instances : a dictionary that maps from input rate to a list of bolt instances where each list is specifies the number of instances for a particular topology
    """
    executed_topologies = []
    commands = []
    print(bolt_instances)
    for i in range(len(input_rates)):
        # get bolt_instances for topo with this input_rate
        bolt_instances_for_ir = bolt_instances[input_rates[i]]
        # convert bolt_instances to str 
        bolt_instances_for_ir = str(bolt_instances_for_ir)[1:-1].replace(" ", "")

        jar_path  = os.getcwd() + "/" + jar_name
        topo_unique_name = in_topo_name + "_" + str(input_rates[i])

        executed_topologies.append(topo_unique_name)
                
        if riot:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
        else:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " 1 " + pi_outdir
                    
        commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]) + " " + str(num_workers) + " " + bolt_instances_for_ir)
        
        print "Running experiment:"
        print commands[i] + "\n"
        process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        #print output

    return (commands, executed_topologies)


def launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, riot, num_workers):
    executed_topologies = []
    commands = []

    for i in range(len(input_rates)):
        jar_path  = os.getcwd() + "/" + jar_name
        topo_unique_name = in_topo_name + "_" + str(input_rates[i])

        executed_topologies.append(topo_unique_name)
                
        if riot:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name  + " " + data_file + " 1 1 " + pi_outdir + " " + prop_file + " " + topo_unique_name
        else:
            command_pre = storm_exe + " jar " + jar_path + " " + topo + " C " +  topo_unique_name + " 1 " + pi_outdir
        
        commands.append(command_pre + " " + str(input_rates[i]) + " " + str(num_events[i]) + " " + str(num_workers))
        
        print "Running experiment:"
        print commands[i] + "\n"
        process = subprocess.Popen(commands[i].split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        #print output

    return (commands, executed_topologies)

def get_spout_sink_files(pi_outdir, executed_topologies, riot):
    spout_files = []
    sink_files = []
    bp_files = []

    print ("Executed Topologies: ")
    print (executed_topologies)
    for i in range(len(executed_topologies)):
        if riot:
            spout_files.append(pi_outdir + "/" + "spout-" + executed_topologies[i] + "-1-1.0.log")
            sink_files.append(pi_outdir + "/" + "sink-" + executed_topologies[i] + "-1-1.0.log")
        else:
            spout_files.append(pi_outdir + "/" + "spout-" + executed_topologies[i] + "-1-.log")
            sink_files.append(pi_outdir + "/" + "sink-" + executed_topologies[i] + "-1-.log")
        
        if bpMonitor:
            bp_files.append(pi_outdir + "/" + "back_pressure-" + "spout-" + executed_topologies[i] + "-1-1.0.log")

    print ("Spout, sink and backpressure files on devices: ")
    print (spout_files)
    print (sink_files)
    print (bp_files)

    return  (spout_files, sink_files, bp_files)


def get_spout_sink_devices(in_topo_name, input_rates, c2wm):
    """
    Return the device/host the spout and sink bolts are running
    """
    # print ("\n\n")
    # print ("c2wm")
    # print (c2wm)

    print ("input rates")
    print (input_rates)

    spout_devices = []
    sink_devices = []
    dev_user = "pi@"
    
    for i,v in enumerate(input_rates):
        print ("Input rate: " + str(v))
        c2wm_ = c2wm[in_topo_name+"_"+str(v)]
        sp_dev = ""
        sink_dev = ""
        # print(c2wm_)
        for k,v in c2wm_.items():
            if 'spout' in k:
                sp_dev = dev_user+v
            if 'sink' in k:
                sink_dev = dev_user+v
        print ("Dev " + sp_dev + " " + sink_dev)

        spout_devices.append(sp_dev)
        sink_devices.append(sink_dev)

    print ("spout files available on devices: ")
    print (spout_devices)
    print ("sink files available on devices: ")
    print (sink_devices)

    return (spout_devices, sink_devices)

def get_spout_sink_log_files(spout_devices, sink_devices, spout_files, sink_files, bp_files, exp_result_dir):
    for i,dev in enumerate(spout_devices):
        process = subprocess.Popen(["scp", dev + ":" + spout_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
        output, error = process.communicate()

    for i,dev in enumerate(sink_devices):
        process = subprocess.Popen(["scp", dev + ":" + sink_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
        output, error = process.communicate()

    if bpMonitor:
        for i,dev in enumerate(spout_devices):
            process = subprocess.Popen(["scp", dev + ":" + bp_files[i], exp_result_dir+"/"], stdout=subprocess.PIPE)
            output, error = process.communicate()


def read_back_pressure(bp_file):
    bp_in_ms = 0
    with open(bp_file, 'r') as f:
        bp_in_ms = int(f.readline())
    return bp_in_ms

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

def get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_experiments, spout_files, sink_files, bp_files, executed_topologies):
    """
    For each experiment, get the spout and sink log files and calculate throughput and latency
    based on those logs. Writes the results to the provided csv file @ location path/csv_file_name
    """
    exp_results={} # topology to result mapping
    csv_file_name=path+"/"+csv_file_name
    shutil.copy(csv_file_name, exp_result_dir)
    os.chdir(exp_result_dir)

    with open (csv_file_name, "a") as csv_file:
        exp_id="experiment-"+jar_name+"-"+in_topo_name + "-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        csv_file.write("\n\n")
        csv_file.write(exp_id)
        csv_file.write("\n")
        for i in range(num_experiments):
            spout_file = spout_files[i].split("/")[-1]
            sink_file = sink_files[i].split("/")[-1]
            if bpMonitor:
                bp_file = bp_files[i].split("/")[-1]
                bp = read_back_pressure(bp_file)
            
            resultObject = script.get_results(executed_topologies[i], spout_file, sink_file)
            results = resultObject.get_csv_rep(in_topo_name)    #results[0] --> Header, results[1] --> value

            res = resultObject.get_data_dict(in_topo_name)
            if bpMonitor:
                res['bp_in_ms'] = bp
            exp_results[executed_topologies[i]] = res
            if i == 0:
                if bpMonitor:
                    __str = "topology-in_rate," + results[0] + ", bp_in_ms"
                else:
                    __str = "topology-in_rate," + results[0]
                csv_file.write(__str)

            csv_file.write("\n")
            csv_file.write(executed_topologies[i] + ",")
            csv_file.write(results[1])
            if bpMonitor:
                csv_file.write("," + str(bp))

    shutil.copy(csv_file_name, path) # move the modified result file back
    return exp_results

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

def run_experiments(jar_name, in_topo_name, csv_file_name, riot, numWorkers, config=None, dfs=None):

    input_rates = input_rates_dict[in_topo_name]
    num_events = num_events_dict[in_topo_name]

    if riot:
        data_file = data_files[in_topo_name]
        prop_file = property_files[in_topo_name]
    else:
        data_file = None
        prop_file = None

    topo = topo_qualified_path[in_topo_name]
    
    exp_result_dir = results_dir[hostname]
    path = paths[hostname]

    os.chdir(path)

    # kill any topologies if running already
    kill_topologies(path)

    # Clean output directories on PIs
    clean_dirs_on_pis(pi_outdir)

    # Run the experiments now.
    if not config:
        commands, executed_topologies = launch_experiments(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, riot, numWorkers)
    else:
        bolt_instances=[list(config) for v in range(num_experiments)]
        ir2bi = dict(zip(input_rates, bolt_instances))
        write_to_csv_file(path, csv_file_name, "\n\n# Topology Configuration: " + str(config) + "\n")
        commands, executed_topologies = launch_tuning_experiment(input_rates, in_topo_name, jar_name, topo, data_file, pi_outdir, prop_file, num_events, ir2bi, riot, numWorkers)
    # Experiments have been started now

    # get names for spout, sink and backpressure files
    spout_files, sink_files, bp_files = get_spout_sink_files(pi_outdir, executed_topologies, riot)
        
    # Wait until the experiments are done on the PIs
    # get capacities for all the launched experiments
    topo_cap_map, topo_lat_map, component_to_worker_map = get_topo_capacity_at_completion(port=PORT, in_topo_name=in_topo_name, num_experiments=num_experiments)
    #print (topo_cap_map)

    # Check which experiment was run on which PI device...
    # devices = get_devices_for_experiments(spout_files)
    spout_devices, sink_devices = get_spout_sink_devices(in_topo_name, input_rates, component_to_worker_map)

    # directory to get resullts of the experiments
    if not os.path.isdir(exp_result_dir):
        os.mkdir(exp_result_dir)

    # Copy result files from the R.PIs to this machine
    # get_log_files_from_supervisors(devices, spout_files, sink_files, exp_result_dir)
    get_spout_sink_log_files(spout_devices, sink_devices, spout_files, sink_files, bp_files, exp_result_dir)

    # output files have been copied to this machine
    # run script on these files to generate results...
    # After this, the results have been logged to the csv file
    _results = get_results(path, csv_file_name, exp_result_dir, jar_name, in_topo_name, num_experiments, spout_files, sink_files, bp_files, executed_topologies)
    
    # results of topology exploration.
    if config:
        # a topology configuration was run with different input rates, getting results here...
        exp_result_file_name = "topo_exploration_res.csv"
        _file = path+"/"+exp_result_file_name
        write_to_csv_file(path, exp_result_file_name, "\nMeasured Throughput and Latency:\n")
        results_df = pd.DataFrame(_results)
        for col in results_df.columns:
            results_df[col] = pd.to_numeric(results_df[col])
        results_df = results_df.round(2)
        results_df.to_csv(_file, mode='a', sep='\t', encoding='utf-8')
        dfs[str(config)] = results_df


    # get all the spout/sink logs and generated images and archive them.
    archive_results(path, jar_name, in_topo_name)

    # kill all the running topologies on PIs
    kill_topologies(path)

    # change directory back...
    os.chdir(path)
    shutil.rmtree(exp_result_dir)

def get_unique_configs(ir2bis):
    topo_configs = []
    for k in ir2bis:
        topo_configs.append(ir2bis[k])

    unique_topos=set()
    for i in topo_configs:
        for j in i:
            unique_topos.add(tuple(j))

    topos=[]
    for t in unique_topos:
        if t:
            topos.append(list(t))
    return topos

def save_iterator(_file, _list):
    with open(_file, 'w') as f:
        for item in _list:
            f.write(str(item))

def main():
    usage = "python <script_name.py> <jar_file> <topology_name> <csv_file_name> <topology_duration>"
    
    parser = argparse.ArgumentParser(description='run_exp script to run storm experiments')

    parser.add_argument("jar_name")
    parser.add_argument("in_topo_name")
    parser.add_argument("csv_file_name")
    parser.add_argument('--tuning', action="store_true", default=False, help="Running tuning experiment?")
    parser.add_argument('--riot', action="store_true", default=False, help="Running RIOTBench application?")
    parser.add_argument('--explore', action="store_true", default=False, help="Run experiments on tuned topologies with different input rates?")
    parser.add_argument('--explore_with_ir', action="store_true", default=False, help="Run topology tuning experiment while chosing input rates")
    parser.add_argument('--singleStep', action="store_true", default=False, help="Whether to take increase bolt instances only by 1 during topo tuning")
    parser.add_argument('--numIterations', type=int, default=1, choices=xrange(1, 50), help="Nummber of iterations for which to run the experiment")
    parser.add_argument('--numWorkers', type=int, default=1, choices=xrange(1, 10), help="Nummber of workers to launch for the topology")
    parser.add_argument('--latencyThreshold', type=int, default=50, help="Latency Threshold")
    parser.add_argument('--bpMonitor', action="store_true", default=False, help="Whether backpressure is being monitored")

    args = parser.parse_args()

    jar_name = args.jar_name
    in_topo_name = args.in_topo_name
    csv_file_name = args.csv_file_name
    tuning = args.tuning
    riot = args.riot
    explore = args.explore
    singleStep = args.singleStep
    numIterations = args.numIterations
    numWorkers = args.numWorkers
    global bpMonitor
    bpMonitor = args.bpMonitor

    if in_topo_name not in valid_topos:
        print "not a valid topology name."
        print "can only execute " + str (valid_topos)
        exit(-1)

    if args.explore_with_ir:
        explore_with_input_rates(jar_name, in_topo_name, csv_file_name, riot, 
            singleStep, numIterations, numWorkers, args.latencyThreshold)
        exit()

    if not tuning:
        run_experiments(jar_name, in_topo_name, csv_file_name, riot, numWorkers)
    else:
        ir2bis = run_tuning_experiment(jar_name, in_topo_name, csv_file_name, riot, singleStep, numIterations, numWorkers)
        unique_topos = get_unique_configs(ir2bis)
        print("\nRunning explored topologies with chosen input rates\n")
        print(unique_topos)
        save_iterator('explored_topologies', unique_topos)

        if explore:
            # Now need to launch experiments for all topology configurations obtained from running the 
            # tuning experiment.
            dfs={}
            for t in unique_topos:
                print ("Running Experiment with: " + str(t))
                run_experiments(jar_name, in_topo_name, csv_file_name, riot, numWorkers, t, dfs)

            print ("\nExploration Complete...\nCalculating ...")
            figs = []
            sps = []
            topo_paths = topo_to_paths[in_topo_name]
            for topo_path in topo_paths:
                fig = plt.figure(figsize=(25,25))
                figs.append(fig)
                sps.append(fig.add_subplot(111))

            print ('Saving Dataframe')
            # dfs.to_csv('dfs.csv')
            print ("\n\n")
            for conf, frame in dfs.items():
                frame=frame.reindex(columns=sorted(frame.columns, key=natural_sort_key))
                print (conf)
                print (frame)
                frame=frame.T
                frame=frame[frame['bp_in_ms']==0]
                for i, sp in enumerate(sps):
                    sp.plot(frame['throughput_' + topo_paths[i]], 
                        frame['latency_' + topo_paths[i]], label=str(conf))
                    sp.legend()

            for i, fig in enumerate(figs):
                sps[i].set_xlabel('throughput')
                sps[i].set_ylabel('latency')
                fig.savefig(in_topo_name+"_"+topo_paths[i]+".png")

if __name__ == "__main__":
    main()