import os
import subprocess
import argparse
import time

from experiments_prop import *

flink_home = os.environ['FLINK_HOME']
flink_exe = flink_home + "/bin/flink"

FNULL = open(os.devnull, 'w')

# note
usage = "python <script_name.py> <jar_file> <job_alias>"
parser = argparse.ArgumentParser(description='to run flink iot-bm experiments')
parser.add_argument("jar_name")
parser.add_argument("job_alias")
args = parser.parse_args()


def kill_running_jobs():
    cmd = os.getcwd() + "/kill_running_jobs.sh"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()


def clean_metrics_log():
    cmd = "rm " + metrics_log_dir + "/*"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "Clean metrics_log directories on Master"

    cmd = "dsh -aM -c rm " + metrics_log_dir + "/*"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "Clean metrics_log directories on PIs"


def run_flink_job(jar_path, target_job_name, input_rate, num_of_data, resource_path, data_file, prop_file):
    flink_command_pre = flink_exe + " run -c " + target_job_name + " -d " + jar_path
    flink_command = flink_command_pre + " -input " + str(input_rate) + " -total " + str(num_of_data) \
                    + " -res_path " + resource_path + " -data_file " + data_file + " -prop_file " + prop_file

    print "Running experiment:"
    print flink_command + "\n"
    process = subprocess.Popen(flink_command.split(), stdout=subprocess.PIPE)
    # process = subprocess.Popen(flink_command.split())
    output, error = process.communicate()


# def wait_for_job_completion():

def archive_job_metrics(unique_exp_job_log_name):
    cmd = "dsh -aM -c mkdir " + metrics_log_archive_dir + "/" + unique_exp_job_log_name + \
          " & cp " + metrics_log_dir + "/* " + metrics_log_archive_dir + "/" + unique_exp_job_log_name + "/"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is None:
        print "archive metrics log on PIs"


# def colletc_exp_results_on_pis(unique_exp_job_log_name):
#     cmd = "dsh -aM -c scp " + metrics_log_archive_dir + "/" +  + \
#            " & cp " + metrics_log_dir + "/* " + metrics_log_archive_dir + "/" + unique_exp_job_log_name + "/"
#     process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
#     output, error = process.communicate()
#     if error is None:
#         print "archive metrics log on PIs"

# run experiments
def run_experiments(jar_name, job_alias):
    jar_path = os.getcwd() + "/" + jar_name

    target_job_name = target_job_names[job_alias]
    input_rates = input_rates_dict[job_alias]
    nums_of_data = nums_of_data_dict[job_alias]
    data_file = data_files[job_alias]
    prop_file = prop_files[job_alias]

    # run a job:
    input_rate = input_rates[0]
    num_of_data = nums_of_data[0]

    kill_running_jobs()
    clean_metrics_log()
    unique_exp_job_name = job_alias + "-" + str(input_rate) + "-" + str(num_of_data) + "-" + time.strftime(
        "%m.%d-%H.%M", time.localtime())
    run_flink_job(jar_path, target_job_name, input_rate, num_of_data,
                  resource_path, data_file, prop_file)
    # wait_for_job_completion()
    time.sleep(100)
    print "after running"
    archive_job_metrics(unique_exp_job_name)
    kill_running_jobs()

    # after all experiments done
    # colletc_exp_results_on_pis()


def main():
    if args.job_alias not in valid_job_alias:
        print "not a valid job name."
        print "can only execute " + str(valid_job_alias)
        exit(-1)

    run_experiments(args.jar_name, args.job_alias)

    # parse_results()
    # archive_results()


if __name__ == "__main__":
    main()
