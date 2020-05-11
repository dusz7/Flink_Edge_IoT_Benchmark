valid_job_alias = ["etl", "pred", "stats", "train"]

# input_rate & num_of_data
etl_input_rates = [600, 700, 800, 900, 1000, 1100]
etl_nums_of_data = [144000, 168000, 192000, 216000, 240000, 264000]

etl_h_input_rates = [1200, 1300, 1350, 1400, 1450, 1500]
etl_h_nums_of_data = [288000, 312000, 324000, 336000, 348000, 360000]

#################################################

pred_input_rates = [200, 400, 600, 800, 1000, 1200]
pred_nums_of_data = [48000, 96000, 144000, 192000, 240000, 288000]

pred_h_input_rates = [2200, 2201, 2202, 2203, 2204, 2205]
pred_h_nums_of_data = [528000, 528000, 528000, 528000, 528000, 528000]

#################################################

# stats_input_rates = [20, 30]
# stats_nums_of_data = [800, 1200]
stats_input_rates = [10, 30, 50, 70, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100]
stats_nums_of_data = [2000, 6000, 10000, 14000, 18000, 20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000,
                      180000, 200000, 220000]

stats_h_input_rates = [250, 300, 350, 400, 450, 500]
stats_h_nums_of_data = [60000, 72000, 84000, 96000, 108000, 120000]

#################################################

train_input_rates = [30, 40, 50, 60, 70, 80]
train_nums_of_data = [8100, 10800, 13500, 16200, 18900, 21600]

train_h_input_rates = [90, 100, 110, 120, 130, 140]
train_h_nums_of_data = [31500, 27000, 29700, 32400, 35100, 37800]

input_rates_dict = {
    "etl": etl_input_rates,
    "etl_h": etl_h_input_rates,
    "pred": pred_input_rates,
    "pred_h": pred_h_input_rates,
    "stats": stats_input_rates,
    "stats_h": stats_h_input_rates,
    "train": train_input_rates,
    "train_h": train_h_input_rates
}

nums_of_data_dict = {
    "etl": etl_nums_of_data,
    "etl_h": etl_h_nums_of_data,
    "pred": pred_nums_of_data,
    "pred_h": pred_h_nums_of_data,
    "stats": stats_nums_of_data,
    "stats_h": stats_h_nums_of_data,
    "train": train_nums_of_data,
    "train_h": train_h_nums_of_data
}

# data_file & prop_file
resource_path = "/usr/local/etc/flink-remote/bm_files/bm_resources"

data_files = {
    "etl": "SYS_sample_data_senml.csv",
    "pred": "SYS_sample_data_senml.csv",
    "stats": "train_input_data.csv",
    "train": "inputFileForTimerSpout-CITY_NoAz.csv"
}

prop_files = {
    "etl": "my_etl.properties",
    "pred": "my_prediction.properties",
    "stats": "my_stats.properties",
    "train": "my_train.properties"
}

# job_class
target_job_names = {
    "etl": "in.hitcps.iot_edge.bm.flink.jobs.ETLJob",
    "pred": "in.hitcps.iot_edge.bm.flink.jobs.PredictionJob",
    "stats": "in.hitcps.iot_edge.bm.flink.jobs.StatsJob",
    "train": "in.hitcps.iot_edge.bm.flink.jobs.TrainJob"
}

# metrics_log_path
metrics_log_dir = "/usr/local/etc/flink-remote/bm_files/metrics_logs"

# exp_metrics_log_save_dir
exp_results_archive_dir = "/usr/local/etc/flink-remote/bm_files/bm_results"

# exp_results_local_dir
exp_results_local_dir = "/Users/craig/Projects/edgeStreamingForIoT/bm_results"

# exp_results_summ_file_name
exp_results_summ_file = "summary.csv"

# flink_rasp_hosts
rasp_hosts = ["flink_rasp1"]

# end_experiment
PORT = 38997

# experiment_analyze
exp_ids = ["job_name", "execution_time", "input_rate", "num_of_data"]
throughput_metrics = ["throughput"]
latency_metrics = ["latency_mean", "latency_min", "latency_p10", "latency_p25", "latency_p50", "latency_p75",
                   "latency_p95", "latency_p98", "latency_p99", "latency_p999", "latency_max"]
exp_metrics_head = exp_ids + throughput_metrics + latency_metrics
