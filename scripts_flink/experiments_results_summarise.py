import os
import argparse
import re
import csv
import numpy as np

from experiments_prop import *

# note
usage = "python <script_name.py> <job_alias> <start_time>"
parser = argparse.ArgumentParser(description='to summarize flink iot-bm experiments result')
parser.add_argument("job_alias")
parser.add_argument("start_time")
parser.add_argument("--ExecutionTimes", type=int, default=1, choices=xrange(1, 10), help="Experiment Execution times")
args = parser.parse_args()

def get_throughput_from_section(log_section):
    # print log_section[0]
    sink_throughput_dict = {}
    for line in log_section:
        if re.search(r"Sink:.*\d+numRecordsInPerSecond", line) != None:
            temp = line.split(":")
            sink_seq = re.findall(r"\b.*\d+", temp[1])[0]
            sink_throughput_dict[sink_seq] = float(temp[2])
    total_throughput = 0
    for throughput in sink_throughput_dict.values():
        total_throughput += throughput
    return total_throughput

def get_latency_from_section(log_section):
    global latencys
    count = 0
    for line in log_section:
        if "MyMetricslatency" in line:
            count += 1
            # print line
            temp = line.split(":")
            content = re.findall(r"\b\d+\.?\d*",temp[2])
            content_num = [float(x) for x in content]
            if count == 1:
                latencys = np.array(content_num)
            else:
                latencys += np.array(content_num)
    latencys /= count
    return latencys.tolist()

def get_latency_throughput_from_file(file_name):
    f = open(file_name)
    lines = f.readlines()

    # analyse_part
    start_index = lines.index("---------- records counter ----------\n", len(lines)/2)
    end_index = lines.index("---------- records counter ----------\n", len(lines)*3/4)

    an_lines = lines[start_index: end_index-3]
    first_index = an_lines.index("---------- records counter ----------\n")
    an_lines.reverse()
    last_index = len(an_lines) - an_lines.index("---------- records counter ----------\n") - 1
    an_lines.reverse()

    now_index = first_index
    throughputs = []
    while now_index < last_index:
        next_index = an_lines.index("---------- records counter ----------\n", now_index + 1)
        throughputs.append( get_throughput_from_section(an_lines[now_index: next_index - 3]) )
        now_index = next_index

    # last section
    throughputs.append( get_throughput_from_section(an_lines[now_index:]) )
    throughputs_array = np.array(throughputs)
    throughput = throughputs_array.mean()
    # print "throughput: " + str(throughput)
    latency_hist = get_latency_from_section(an_lines[now_index:])
    return throughput, latency_hist

def open_summarize_archive_file(sum_file_name):
    with open(sum_file_name,'wb') as f:
        csv_write = csv.writer(f)
        csv_head = exp_metrics_head
        csv_write.writerow(csv_head)

def write_throughput_latency_in_file(sum_file_name, job_alias, exe_time, input_rate, num_of_data, throughput, latency_hist):
    with open(sum_file_name,'a+') as f:
        csv_write = csv.writer(f)
        csv_row = [job_alias, exe_time, input_rate, num_of_data]
        csv_row.append(throughput)
        csv_row += latency_hist
        csv_write.writerow(csv_row)

def summarize_exp(job_alias, start_time, exe_time):
    # different devices
    for rasp_host in rasp_hosts:
        # get sum_file_name
        unique_exp_name = job_alias + "-" + start_time + "_" + rasp_host
        summ_file_name = exp_results_local_dir + "/" + unique_exp_name + "/" + exp_results_summ_file
        open_summarize_archive_file(summ_file_name)

        input_rates = input_rates_dict[job_alias]
        nums_of_data = nums_of_data_dict[job_alias]

        for i in range(len(input_rates)):
            input_rate = input_rates[i]
            num_of_data = nums_of_data[i]
            throughputs = []
            latency_hists = []
            for t in range(1, exe_time + 1):
                unique_exp_job_name = job_alias + "-" + str(input_rate) + "-" + str(num_of_data) + "-t" + str(t)
                latency_throughput_file_name = exp_results_local_dir + "/" + unique_exp_name + "/" + unique_exp_job_name + "/" + "latency_throughput.txt"
                if os.path.exists(latency_throughput_file_name):
                    throughput, latency_hist = get_latency_throughput_from_file(latency_throughput_file_name)
                    throughputs.append(throughput)
                    latency_hists.append(latency_hist)
                    write_throughput_latency_in_file(summ_file_name, job_alias, t, input_rate, num_of_data, throughput, latency_hist)
            total_mean_throughput = np.mean(np.array(throughputs))
            total_mean_latency_hist = np.mean(np.array(latency_hists), axis=0).tolist()
            write_throughput_latency_in_file(summ_file_name, job_alias, -1, input_rate, num_of_data, total_mean_throughput, total_mean_latency_hist)


def main():
    if args.job_alias not in valid_job_alias:
        print "not a valid job name."
        print "can only execute " + str(valid_job_alias)
        exit(-1)
    summarize_exp(args.job_alias, args.start_time, args.ExecutionTimes)

if __name__ == "__main__":
    main()

# run example:
# python experiments_results_summarise.py stats 05.11-05.11 --ExecutionTimes 2