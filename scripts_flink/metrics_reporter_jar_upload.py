import os
import subprocess
import time

flink_home = os.environ['FLINK_HOME']
flink_lib = flink_home + "/lib"

FNULL = open(os.devnull, 'w')

def package_metrics_reporter_jar():
    retval = os.getcwd()
    project_path = retval+"/../modules/flink-metrics-reporter"
    os.chdir(project_path)

    cmd = "mvn -DskipTests clean package"
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    os.chdir(retval)

def get_metrics_reporter_jar():
    cmd = "cp ../modules/flink-metrics-reporter/target/original-flink-metrics-reporter-1.0-SNAPSHOT.jar flink-metrics-my.jar"
    process = subprocess.Popen(cmd.split(), stdout=FNULL, stderr=subprocess.STDOUT)
    output, error = process.communicate()

def upload_jar():
    cmd = "sudo cp flink-metrics-my.jar " + flink_lib
    process = subprocess.Popen(cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(1)
    output, error = process.communicate()
    process.stdin.close()

    # remote_cmd = "sudo scp flink-metrics-my.jar pi@192.168.88.237:/usr/local/etc/flink-remote/lib/"
    remote_cmd = "sudo scp flink-metrics-my.jar pi@192.168.88.237:/usr/local/etc/flink-remote/lib/"
    process = subprocess.Popen(remote_cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, error = process.communicate()
    if error is not None:
        print error

def main():
    package_metrics_reporter_jar()
    get_metrics_reporter_jar()
    upload_jar()

if __name__ == "__main__":
    main()