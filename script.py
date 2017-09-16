#################################################################################
#   Generate metrics after topology's execution
#   Execute as:
#   python <script_name.py> <topology_name> /path/to/spout_log /path/to/sink_log 
#   Outputs: Input Rate, Throughput, Average Latency
#################################################################################

import sys
import numpy
from datetime import datetime
import time
import matplotlib
matplotlib.use('Agg')

import matplotlib.mlab as mlab
import matplotlib.pyplot as plt


def getInputRate(inFile):
    return getRate(inFile, 3, 5)

def getThroughput(outFile):
    return getRate(outFile, 3, 4)


def getRate(_file, timestampIndex, msgIdIndex):
    with open(_file, "r") as f:
        first = f.readline().split(",")
        for line in f:
            pass

        last = line.split(",")
        startTime = long(first[timestampIndex])
        endTime = long(last[timestampIndex])
        startMsg = long(first[msgIdIndex])
        endMsg = long(last[msgIdIndex])
        rate = float((endMsg - startMsg)) / ((endTime-startTime)/1000)
    return rate

def get_etl_topo_throughput(_file, timestampIndex, msgIdIndex, path1, path2):
    with open(_file, "r") as f:
        firstAz = ""
        firstMq = ""
        lastAz = ""
        lastMq = ""
        for line in f:
            if path1 in line and firstAz == "":
                firstAz = line
            if path2 in line and firstMq == "":
                firstMq = line
            if firstAz != "" and firstMq != "":
                break
        for line in f:
            if path1 in line:
                lastAz = line
            elif path2 in line:
                lastMq = line
    firstAz = firstAz.split(",")
    lastAz = lastAz.split(",")
    firstMq = firstMq.split(",")
    lastMq = lastMq.split(",")
    azTime = long(lastAz[timestampIndex]) - long(firstAz[timestampIndex])
    mqTime = long(lastMq[timestampIndex]) - long(firstMq[timestampIndex])
    azMsgs = long(lastAz[msgIdIndex]) - long(firstAz[msgIdIndex])
    mqMsgs = long(lastMq[msgIdIndex]) - long(firstMq[msgIdIndex])
    azThroughput = float(azMsgs) / (azTime/1000)
    mqThroughput = float(mqMsgs) / (mqTime/1000)
    
    return (azThroughput, mqThroughput)

def get_etl_topo_latency(topology_name, in_file, out_file, in_ts_index, in_msg_index, out_ts_index, out_msg_index, path1, path2):
    in_map = {}
    az_out_map = {}
    mq_out_map = {}
    az_latency_map = {}
    mq_latency_map = {}
    with open(in_file, "r") as f:
        for line in f:
            line_arr = line.split(",")
            ts = line_arr[in_ts_index]
            msg_id = line_arr[in_msg_index].rstrip()
            in_map[msg_id] = ts
    
    with open(out_file, "r") as f:
        for line in f:
            line_arr = line.split(",")
            if path1 in line:
                ts = line_arr[out_ts_index]
                msg_id = line_arr[out_msg_index]
                az_out_map[msg_id] = ts

            elif path2 in line:
                ts = line_arr[out_ts_index]
                msg_id = line_arr[out_msg_index]
                mq_out_map[msg_id] = ts
    az_total_latency = 0
    mq_total_latency = 0
    az_count = 0
    mq_count = 0
    az_latencies = []
    mq_latencies = []
    for key in in_map:
        start_time = long(in_map[key])
        try:   
            az_end_time = long(az_out_map[key])
            if (az_end_time):
                latency = (az_end_time - start_time)
                az_total_latency += latency
                az_latencies.append(latency)
                az_count+=1
        except:
            pass
        try:
            mq_end_time = long(mq_out_map[key])
            if (mq_end_time):
                latency = (mq_end_time - start_time)
                mq_total_latency += latency
                mq_latencies.append(latency)
                mq_count+=1
        except:
            pass
    #print len(az_latencies)
    #print az_count
    mq_av_latency = mq_total_latency / mq_count
    az_av_latency = az_total_latency / az_count
    #print len(mq_out_map)
    #print len(az_out_map)

    az_latencies = numpy.asarray(az_latencies)
    mq_latencies = numpy.asarray(mq_latencies)
    
    n, bins, patches = plt.hist(az_latencies, 50, facecolor='green')
    plt.savefig(topology_name + "-" + path1 + "-" + time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    
    n, bins, patches = plt.hist(mq_latencies, 50, facecolor='green')    
    plt.savefig(topology_name + "-" + path2 + "-" + time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))

    return (az_av_latency, mq_av_latency)
    

def getLatency(in_file, out_file, in_ts_index, in_msg_index, out_ts_index, out_msg_index):
    in_map = {}
    out_map = {}
    latency_map = {}
    with open(in_file, "r") as f:
        for line in f:
            line_arr = line.split(",")
            ts = line_arr[in_ts_index]
            msg_id = line_arr[in_msg_index]
            in_map[msg_id] = ts
    
    with open(out_file, "r") as f:
        for line in f:
            line_arr = line.split(",")
            ts = line_arr[out_ts_index]
            msg_id = line_arr[out_msg_index]
            out_map[msg_id] = ts
    
    for key in in_map:
        try:
            start_time = long(in_map[key])
            end_time = long(out_map[key])
            if (end_time):
                latency_map[key] = (end_time - start_time)
        except:
            pass
    
    total_latency = 0
    for key in latency_map:
        total_latency = total_latency + latency_map[key]
    return total_latency / len(latency_map)

def get_stats_topo_throughput(sink_file, timestampIndex):
    with open(sink_file, "r") as f:
        first = f.readline().split(",")
        num_msgs=1
        for line in f:
            num_msgs+=1
#        print num_msgs
        last = line.split(",")
        startTime = long(first[timestampIndex])
        endTime = long(last[timestampIndex])
        rate = float(num_msgs) / ((endTime-startTime)/1000)
    return rate

class Results(object):
    def __init__(self, topo_name, input_rate, throughput, latencies):
	self.topo_naem = topo_name
	self.input_rate = input_rate
	self.throughput = throughput
	self.latencies = latencies
    
    def get_paths(self, topo_name):
	if topo_name.startswith("etl"):
	    return ("azure_insert_path", "publish_path")
	elif topo_name.startswith("wordcount"):
	    return ("wordcount")
	elif topo_name.startswith(("pred", "train")):
            return ("DTC_path", "MLR_path")
	elif topo_name.startswith("stat"):
            return ("stat")
	else:
	    return None
    
    def get_top_row(self, topo_name):
	row = "input_rate,"
	paths = self.get_paths(topo_name)
	for path in paths:
	    row = row + "throughput_" + path + ","
	for path in paths:
	    row = row + "latency_" + path + ","
	return row[0:-1]

    def get_csv_data(self, topo_name):
	data = str(self.input_rate) + ","
	for tp in self.throughput:
            data = data + str(tp) + ","
	for lat in self.latencies:
	    data = data + str(lat) + ","
	return data[0:-1]

    def get_csv_rep(self, topo_name):
	return [self.get_top_row(topo_name), self.get_csv_data(topo_name)]

def get_results(topology_name, spoutFile, sinkFile):
    input_rate = 0
    throughputs = []
    latencies = []
    if topology_name.startswith("etl"):
	input_rate = getInputRate(spoutFile)
        print "Input Rate (msgs/sec): " + str(input_rate)
        (tp0, tp1) = get_etl_topo_throughput(sinkFile, 3, 4, "AzureInsert", "PublishBolt")
        (l0, l1) = get_etl_topo_latency(topology_name, spoutFile, sinkFile, 3, 5, 3 ,4, "AzureInsert", "PublishBolt")
	throughputs.append(tp0)
	throughputs.append(tp1)
	latencies.append(l0)
	latencies.append(l1)
	print topology_name + ": AzureInsert path Throughput (msgs/sec): " + str(throughputs[0])
        print topology_name + ": PublishBolt path Throughput (msgs/sec): " + str(throughputs[1])
        print topology_name + ": AzureInsert path Latency (msec): " + str(latencies[0])
        print topology_name + ": PublishBolt path Latency (msec): " + str(latencies[1])

    elif  topology_name.startswith("wordcount"):
	input_rate = getInputRate(spoutFile)
	tp0 = getThroughput(sinkFile)
	l0 = getLatency(spoutFile, sinkFile, 3, 5, 3 ,4)
	throughputs.append(tp0)
	latencies.append(l0)
        print "Input Rate (msgs/sec): " + str(input_rate)
        print "Throughput (msgs/sec): " + str(throughputs[0])
        print "Average Latency (ms): " + str(latencies[0])
    elif topology_name.startswith("pred"):
	input_rate = getInputRate(spoutFile)
        print "Input Rate (msgs/sec): " + str(input_rate)
        (tp0, tp1) = get_etl_topo_throughput(sinkFile, 3, 4, "DTC", "MLR")
        (l0, l1) = get_etl_topo_latency(topology_name, spoutFile, sinkFile, 3, 5, 3 ,4, "DTC", "MLR")
	throughputs.append(tp0)
	throughputs.append(tp1)
	latencies.append(l0)
	latencies.append(l1)
    elif topology_name.startswith("stat"):
	input_rate = getInputRate(spoutFile)
	tp0 = get_stats_topo_throughput(sinkFile,3)
	l0 = getLatency(spoutFile, sinkFile, 3, 5, 3 ,4)
	throughputs.append(tp0)
	latencies.append(l0)
        print "Input Rate (msgs/sec): " + str(input_rate)
        print "Throughput (msgs/sec): " + str(throughputs[0])
        print "Average Latency (ms): " + str(latencies[0])
    elif topology_name.startswith("train"):
	input_rate = getInputRate(spoutFile)
        print "Input Rate (msgs/sec): " + str(input_rate)
        (tp0, tp1) = get_etl_topo_throughput(sinkFile, 3, 4, "DTC", "MLR")
        (l0, l1) = get_etl_topo_latency(topology_name, spoutFile, sinkFile, 3, 5, 3 ,4, "DTC", "MLR")
	throughputs.append(tp0)
	throughputs.append(tp1)
	latencies.append(l0)
	latencies.append(l1)

    return Results(topology_name, input_rate, throughputs, latencies)
    #return (input_rate, throughputs, latencies)

def main():
    usage = "python <script_name.py> <topology_name> </path/to/spout_log_file> </path/to/sink_log_file>"

    if len(sys.argv) != 4:
        print "Invalid number of arguments. See usage below."
        print usage
        exit(-1)

    topology_name = sys.argv[1]
    spoutFile = sys.argv[2]
    sinkFile = sys.argv[3]
    
    get_results(topology_name, spoutFile, sinkFile)

if __name__ == "__main__":
    main()


