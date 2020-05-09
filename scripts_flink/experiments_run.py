import os
import subprocess
import argparse
import time
import socket

from experiments_prop import *

flink_home = os.environ['FLINK_HOME']
flink_exe = flink_home + "/bin/flink"

FNULL = open(os.devnull, 'w')

# note
usage = "python <script_name.py> <jar_file> <job_alias>"
parser = argparse.ArgumentParser(description='to run flink iot-bm experiments')
parser.add_argument("jar_name")
parser.add_argument("job_alias")
parser.add_argument("--ExecutionTimes", type=int, default=1, choices=xrange(1, 10), help="Experiment Execution times")
args = parser.parse_args()


def kill_running_jobs():
    cmd = os.getcwd() + "/kill_running_jobs.sh"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()


def clean_metrics_log():
    # cmd = "rm " + metrics_log_dir + "/*"
    # process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    # output, error = process.communicate()
    # if error is None:
    #     print "Clean metrics_log directories on Master"

    cmd = "dsh -aM -c rm " + metrics_log_dir + "/*"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "  cleaned metrics_log directories on PIs"


def run_flink_job(jar_path, target_job_name, input_rate, num_of_data, resource_path, data_file, prop_file):
    flink_command_pre = flink_exe + " run -c " + target_job_name + " -d " + jar_path
    flink_command = flink_command_pre + " -input " + str(input_rate) + " -total " + str(num_of_data) \
                    + " -res_path " + resource_path + " -data_file " + data_file + " -prop_file " + prop_file

    # print "Running experiment:"
    # print flink_command
    print "  +++++ input_throughput: " + str(input_rate) + "   total_num_of_data: " + str(num_of_data)
    print "  +++++ started running the job at: " + time.strftime("%H.%M.%S", time.localtime())

    process = subprocess.Popen(flink_command.split(), stdout=subprocess.PIPE)
    # process = subprocess.Popen(flink_command.split())
    output, error = process.communicate()


def wait_for_job_completion(start_time, port=38999):
    host = ''
    # create a socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # AF_INET-> IPv4, SOCK_STREAM-> TCP

    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((host, port))
    except socket.error as msg:
        print ('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
    #Start listening on socket
    s.listen(10)

    #wait to accept a connection - blocking call
    conn, addr = s.accept()

    s.close()
    print "  +++++ completed the job, using complete ({:.4f} min)".format((time.time() - start_time)/60)

def create_exp_results_dir(unique_exp_name):
    cmd = "dsh -aM -c mkdir " + metrics_log_archive_dir + "/" + unique_exp_name
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "Created exp_results directories"

def archive_job_metrics(unique_exp_job_name, unique_exp_name):
    cmd = "dsh -aM -c mkdir " + metrics_log_archive_dir + "/" + unique_exp_name + "/" + unique_exp_job_name + \
          " & cp " + metrics_log_dir + "/* " + metrics_log_archive_dir + "/" + unique_exp_name + "/" + unique_exp_job_name + "/"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "  +++++ archived metrics log on PIs"

# def colletc_exp_results_on_pis(unique_exp_job_log_name):
#     cmd = "dsh -aM -c scp " + metrics_log_archive_dir + "/" +  + \
#            " & cp " + metrics_log_dir + "/* " + metrics_log_archive_dir + "/" + unique_exp_job_log_name + "/"
#     process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
#     output, error = process.communicate()
#     if error is None:
#         print "archive metrics log on PIs"

# run experiments
def run_experiments(jar_name, job_alias, execution_time, unique_exp_name):
    jar_path = os.getcwd() + "/" + jar_name

    target_job_name = target_job_names[job_alias]
    input_rates = input_rates_dict[job_alias]
    nums_of_data = nums_of_data_dict[job_alias]
    data_file = data_files[job_alias]
    prop_file = prop_files[job_alias]

    for i in range(len(input_rates)):
        print "--------------------------------------------"
        # run a job:
        input_rate = input_rates[i]
        num_of_data = nums_of_data[i]
        # prepare job
        kill_running_jobs()
        clean_metrics_log()
        print "  +++++ prepared this job"
        # start job
        unique_exp_job_name = job_alias + "-" + str(input_rate) + "-" + str(num_of_data) + "-t" + str(execution_time)
        run_flink_job(jar_path, target_job_name, input_rate, num_of_data,
                      resource_path, data_file, prop_file)
        # blocking wait
        wait_for_job_completion(port=PORT, start_time=time.time())
        # finish job
        archive_job_metrics(unique_exp_job_name, unique_exp_name)
        kill_running_jobs()
        print "  +++++ canceled the running job"
        print "  +++++ +++++ +++++ +++++ +++++"
        time.sleep(10)
        pring ""

    print "one time of experiments execute completed!"

    # after all experiments done
    # colletc_exp_results_on_pis()


def main():
    if args.job_alias not in valid_job_alias:
        print "not a valid job name."
        print "can only execute " + str(valid_job_alias)
        exit(-1)

    t = args.ExecutionTimes
    print "Starting the " + str(args.job_alias) + " experiments, total " + str(t) + " times..."

    unique_exp_name = args.job_alias + "-" + time.strftime("%m.%d-%H.%M", time.localtime())
    create_exp_results_dir(unique_exp_name)

    for i in range(t):
        print "***************************************************"
        print "Now the " + str(i+1) + " time's execution..."
        run_experiments(args.jar_name, args.job_alias, i+1, unique_exp_name)

    # parse_results()
    # archive_results()

if __name__ == "__main__":
    main()
