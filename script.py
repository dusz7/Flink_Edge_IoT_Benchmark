################################################################
#   Generate metrics after topology's execution
#   Execute as:
#   python <script_name.py> /path/to/spout_log /path/to/sink_log 
#   Outputs: Input Rate, Throughput, Average Latency
################################################################

import sys

if len(sys.argv) != 4:
    exit(-1)

topology_name = sys.argv[1]
spoutFile = sys.argv[2]
sinkFile = sys.argv[3]

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

def get_etl_topo_throughput(_file, timestampIndex, msgIdIndex):
    with open(_file, "r") as f:
        firstAz = ""
        firstMq = ""
        lastAz = ""
        lastMq = ""
        for line in f:
            if "AzureInsert" in line and firstAz == "":
                firstAz = line
            if "PublishBolt" in line and firstMq == "":
                firstMq = line
            if firstAz != "" and firstMq != "":
                break
        for line in f:
            if "AzureInsert" in line:
                lastAz = line
            elif "PublishBolt" in line:
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

    print "ETLTopology: AzureInsert path Throughput (msgs/sec): " + str(azThroughput)
    print "ETLTopology: MqttPublish path Throughput (msgs/sec): " + str(mqThroughput)

def get_etl_topo_latency(in_file, out_file, in_ts_index, in_msg_index, out_ts_index, out_msg_index):
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
            if "AzureInsert" in line:
                ts = line_arr[out_ts_index]
                msg_id = line_arr[out_msg_index]
                az_out_map[msg_id] = ts

            elif "PublishBolt" in line:
                ts = line_arr[out_ts_index]
                msg_id = line_arr[out_msg_index]
                mq_out_map[msg_id] = ts
    az_total_latency = 0
    mq_total_latency = 0
    for key in in_map:
        start_time = long(in_map[key])
        try:   
            az_end_time = long(az_out_map[key])
            if (az_end_time):
                az_total_latency += (az_end_time - start_time)
        except:
            pass
        try:
            mq_end_time = long(mq_out_map[key])
            if (mq_end_time):
                mq_total_latency += (mq_end_time - start_time)
        except:
            pass
    mq_av_latency = mq_total_latency / len(mq_out_map)
    az_av_latency = az_total_latency / len(az_out_map)
    print "ETLTopology: AzureInsert path Latency (msec): " + str(az_av_latency)
    print "ETLTopology: MqttPublish path Latency (msec): " + str(mq_av_latency)

    

def getLatency(in_file, out_file, in_ts_index, in_msg_index, out_ts_index, out_msg_index):
    in_map = {}
    out_map = {}
    latency_map = {}
    with open(in_file, "r") as f:
        for line in f:
            line_arr = line.split(",")
            ts = line_arr[in_ts_index]
            msg_id = line_arr[in_msg_index].rstrip()
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


if topology_name == "ETLTopology":
    print "Input Rate (msgs/sec): " + str(getInputRate(spoutFile))
    get_etl_topo_throughput(sinkFile, 3, 4)
    get_etl_topo_latency(spoutFile, sinkFile, 3, 5, 3 ,4)
elif  topology_name == "WordCountTopology":
    print "Throughput (msgs/sec): " + str(getThroughput(sinkFile))
    print "Average Latency (ms): " + str(getLatency(spoutFile, sinkFile, 3, 5, 3 ,4))

