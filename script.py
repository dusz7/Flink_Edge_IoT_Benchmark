################################################################
#   Generate metrics after topology's execution
#   Execute as:
#   python <script_name.py> /path/to/spout_log /path/to/sink_log 
#   Outputs: Input Rate, Throughput, Average Latency
################################################################

import sys

if len(sys.argv) != 3:
    exit(-1)

spoutFile = sys.argv[1]
sinkFile = sys.argv[2]

#print spoutFile
#print sinkFile

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


print "Input Rate (msgs/sec): " + str(getInputRate(spoutFile))
print "Throughput (msgs/sec): " + str(getThroughput(sinkFile))
print "Average Latency (ms): " + str(getLatency(spoutFile, sinkFile, 3, 5, 3 ,4))
