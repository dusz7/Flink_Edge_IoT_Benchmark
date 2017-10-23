valid_topos = ["etl", "pred", "stat"]
num_experiments = 5

# input rates for etl topology
#etl_input_rates = [ 60, 120, 240, 480, 960] # , 1920
#etl_num_events = [14400,  28800, 57600, 115200, 230400] # , 460800

etl_input_rates = [ 60, 120, 240, 480, 960] # , 1920
etl_num_events = [3600,  7200, 14400, 28800, 57600] # , 460800

# input rates for prediction topology
pred_input_rates = [160, 240, 320, 480, 640, 1280, 2560 ]
pred_num_events = [38400 , 57600, 76800, 115200, 153600, 307200, 614400]

# input rates for stat topology
stat_input_rates = [8, 16, 20, 30, 40]
stat_num_events = [ 1920, 3840, 4800, 9600, 7200]

# experimental input rates for etl topology
etl_test_input_rates = [20, 40, 60, 80, 100, 120, 240]
etl_test_num_events = [4800 , 9600, 14400, 19200, 24000, 28800, 57600]

# experimental input rates for etl topology
test_rate = [20, 40, 60, 80, 100, 120, 240]
test_num_events = [4800, 9600, 14400,19200, 24000, 28800, 57600]


property_files = {
		 "etl" : "etl_topology.properties",
		 "pred" : "tasks_CITY.properties",
		 "stat" : "stats_with_vis_topo.properties",
		 "train" : "iot_train_topo_city.properties"
		 }

topo_qualified_path = {
			"etl" : "in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology2",
			"pred" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTPredictionTopologySYS",
			"stat" : "in.dream_lab.bm.stream_iot.storm.topo.apps.StatsWithVisualizationTopology",
			"train" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTTrainTopologySYS",
			"wordcount" : "in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology"
			}

data_files = {
	      "etl" : "SYS_sample_data_senml.csv",
	      "train" : "inputFileForTimerSpout-CITY.csv",
	      "pred" : "SYS_sample_data_senml.csv",
	      "s'etl' : tat" : "SYS_sample_data_senml.csv"
	     }


etl_bolt_ind = {'SenMlParseBolt' : 0, 'RangeFilterBolt' : 1, 'BloomFilterBolt' : 2, 'InterpolationBolt' : 3, 'JoinBolt' : 4, 'AnnotationBolt' : 5,
						 'AzureInsert' : 6, 'CsvToSenMLBolt' : 7, 'PublishBolt' : 8}

topology_bolts = {'etl' : etl_bolt_ind}


input_rates_dict = {"etl" : etl_input_rates, "pred" : pred_input_rates, "stat": stat_input_rates}
num_events_dict = {"etl" : etl_num_events, "pred":pred_num_events, "stat": stat_num_events}

paths={'toybox':'/home/fuxinwei/iot'}
results_dir={'toybox':'/home/fuxinwei/iot/experiment_results'}

pi_outdir = "/home/pi/topo_run_outdir/reg-SYS"


HOST = ''   
PORT = 38999 
