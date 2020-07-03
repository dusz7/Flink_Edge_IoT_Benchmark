valid_job_alias = ["etl", "pred", "stats", "train"]

# input_rate & num_of_data

etl_input_rates = [700, 900, 1100, 1150, 1200, 1250, 1275, 1300, 1325, 1350, 1375, 1400, 1450]
etl_nums_of_data = [168000, 216000, 264000, 276000, 288000, 300000, 306000, 312000, 318000, 324000, 333000, 336000,
                    348000]
# thread control ...
# etl_input_rates = [700, 1100, 1300, 1400, 1500, 1550, 1600, 1625, 1650, 1700, 1800]
# etl_nums_of_data = [168000, 264000, 312000, 336000, 360000, 372000, 384000, 390000, 396000, 408000, 432000]

#################################################

# pred_input_rates = [200, 400]
# pred_nums_of_data = [6000, 12000]
# pred_input_rates = [30, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100]
# pred_nums_of_data = [7200, 12000, 24000, 48000, 72000, 96000, 120000, 144000, 168000, 192000, 216000, 240000, 264000]
# pred_input_rates = [1100, 1300, 1500, 1700, 1800, 1900, 2000, 2100, 2200, 2300]
# pred_nums_of_data = [264000, 312000, 360000, 408000, 432000, 456000, 480000, 504000, 528000, 552000]

# pred_input_rates = [2400, 2500, 2600, 2700, 2800, 2900, 3000]
# pred_nums_of_data = [576000, 600000, 624000, 648000, 672000, 696000, 720000]

# pred_input_rates = [2600, 3000, 3200, 3400, 3500, 3600, 3650, 3700, 3750, 3800, 3850, 3900, 4000]
# pred_nums_of_data = [624000, 720000, 768000, 816000, 840000, 864000, 876000, 888000, 900000, 912000, 924000, 936000, 960000]

pred_input_rates = [3000,3400,  3600,  3700, 3750, 3800,  3900, 4000, 4100, 4200, 4300]
pred_nums_of_data = [720000,  816000, 864000,  888000, 900000, 912000, 936000, 960000, 984000, 1008000, 1032000]

#################################################

# stats_input_rates = [20, 30]
# stats_nums_of_data = [600, 900]
# 4min
stats_input_rates = [200, 400, 600, 700, 750, 800, 825, 850, 875, 900, 925, 950, 1000]
stats_nums_of_data = [48000, 96000, 144000, 168000, 180000, 192000, 198000, 204000, 210000, 216000, 222000, 228000,
                      240000]

#################################################

# train_input_rates = [60, 120]
# train_nums_of_data = [1800, 3600]
train_input_rates = [ 70, 110, 150, 190, 210, 230, 250, 270, 290, 310, 330]
train_nums_of_data = [ 16800, 26400, 36000, 45600, 50400, 55200, 60000, 64800, 69600, 74400, 79200]

input_rates_dict = {
    "etl": etl_input_rates,
    "pred": pred_input_rates,
    "stats": stats_input_rates,
    "train": train_input_rates,
}

nums_of_data_dict = {
    "etl": etl_nums_of_data,
    "pred": pred_nums_of_data,
    "stats": stats_nums_of_data,
    "train": train_nums_of_data,
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
latency_metrics = ["latency_mean", "latency_min", "latency_p5", "latency_p10", "latency_p20", "latency_p25",
                   "latency_p30", "latency_p40", "latency_p50", "latency_p60", "latency_p70", "latency_p75",
                   "latency_p80", "latency_p90", "latency_p95", "latency_p98", "latency_p99", "latency_p999",
                   "latency_max"]
exp_metrics_head = exp_ids + throughput_metrics + latency_metrics
