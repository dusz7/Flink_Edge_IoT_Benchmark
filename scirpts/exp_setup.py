valid_topos = ["etl_l", "etl_h", "pred_l", "pred_h", "stat_l", "stat_h", "train_l", "train_h"]

#################################################

#etl_input_rates = [500,600,700,800,900,1000,1100,1200,1300,1400,1450,1500]
#etl_num_events = [120000,144000,168000,192000,216000,240000,264000,288000,312000,336000,348000,360000]
#etl_num_events = [240000, 288000, 336000, 384000, 432000, 480000, 528000, 576000, 624000, 672000, 696000, 720000]

#etl_l_input_rates = [500,600,700,800,900,1000]
#etl_l_num_events = [240000, 288000, 336000, 384000, 432000, 480000]

#etl_l_input_rates = [1100,1200,1300,1400,1450,1500]
#etl_l_num_events = [528000, 576000, 624000, 672000, 696000, 720000]

#etl_h_input_rates = [1550,1600,1700,1800,1900,2000]
#etl_h_num_events = [744000,768000,816000,864000,912000,960000]

etl_l_input_rates = [600,700,800,900,1000,1100]
etl_l_num_events = [144000,168000,192000,216000,240000,264000]

etl_h_input_rates = [1200,1300,1350,1400,1450,1500]
etl_h_num_events = [288000,312000,324000,336000,348000,360000]

#################################################

#pred_input_rates = [200,400,600,800,1000,1200,1400,1600,1800,2000,2200,2400]
#pred_num_events = [48000,96000,144000,192000,240000,288000,336000,384000,432000,480000,528000,576000]

pred_l_input_rates = [200,400,600,800,1000,1200]
pred_l_num_events = [48000,96000,144000,192000,240000,288000]

#pred_h_input_rates = [1400,1600,1800,2000,2200,2400]
#pred_h_num_events = [336000,384000,432000,480000,528000,576000]

#pred_h_input_rates = [2000,2200,2001,2201,2002,2202]
#pred_h_num_events = [480000,528000,480000,528000,480000,528000]

pred_h_input_rates = [2200,2201,2202,2203,2204,2205]
pred_h_num_events = [528000,528000,528000,528000,528000,528000]

#################################################

#stat_input_rates = [60,80,100,150,180,200,250,300,350,400,450,500]
#stat_num_events = [14400,19200,24000,36000,43200,48000,60000,72000,84000,96000,108000,120000]

stat_l_input_rates = [60,80,100,150,180,200]
stat_l_num_events = [14400,19200,24000,36000,43200,48000]

stat_h_input_rates = [250,300,350,400,450,500]
stat_h_num_events = [60000,72000,84000,96000,108000,120000]

#################################################

#train_input_rates = [10,20,30,40,50,60,70,80,90,100,110,120]
#train_num_events = [2400,4800,7200,9600,12000,14400,16800,19200,21600,24000,26400,28800]
#train_num_events = [4800,9600,14400,19200,24000,28800,33600,38400,43200,48000,52800,57600]

#train_l_input_rates = [10,20,30,40,50,60]
#train_l_num_events = [4800,9600,14400,19200,24000,28800]

#train_h_input_rates = [70,80,90,100,110,120]
#train_h_num_events = [33600,38400,43200,48000,52800,57600]

#train_l_input_rates = [10,20,30,40,50,60]
#train_l_num_events = [2400,4800,7200,9600,12000,14400]

#train_h_input_rates = [70,80,90,100,110,120]
#train_h_num_events = [16800,19200,21600,24000,26400,28800]

#train_l_input_rates = [30,40,50,60,70,80]
#train_l_num_events = [7200,9600,12000,14400,16800,19200]

#train_h_input_rates = [90,100,110,120,130,140]
#train_h_num_events = [21600,24000,26400,28800,31200,33600]

train_l_input_rates = [30,40,50,60,70,80]
train_l_num_events = [8100,10800,13500,16200,18900,21600]

train_h_input_rates = [90,100,110,120,130,140]
train_h_num_events = [24300,27000,29700,32400,35100,37800]

#train_l_input_rates = [40,50,60,70,80,90]
#train_l_num_events = [19200,24000,28800,33600,38400,43200]

#train_h_input_rates = [100,110,120,130,140,150]
#train_h_num_events = [48000,52800,57600,62400,67200,72000]

#################################################

input_rates_dict = {
                    "etl_l" : etl_l_input_rates, 
                    "etl_h" : etl_h_input_rates, 
                    "pred_l" : pred_l_input_rates, 
                    "pred_h" : pred_h_input_rates, 
                    "stat_l": stat_l_input_rates, 
                    "stat_h": stat_h_input_rates, 
                    "train_l" : train_l_input_rates,
                    "train_h" : train_h_input_rates
                    }

num_events_dict = {
                    "etl_l" : etl_l_num_events, 
                    "etl_h" : etl_h_num_events,
                    "pred_l": pred_l_num_events, 
                    "pred_h": pred_h_num_events, 
                    "stat_l": stat_l_num_events, 
                    "stat_h": stat_h_num_events, 
                    "train_l" : train_l_num_events,
                    "train_h" : train_h_num_events
                    }

data_files = {
	        "etl_l" : "SYS_sample_data_senml.csv",
	        "pred_l" : "SYS_sample_data_senml.csv",
	        "stat_l" : "SYS_sample_data_senml.csv",
	        "train_l" : "inputFileForTimerSpout-CITY.csv",
	        "etl_h" : "SYS_sample_data_senml.csv",
	        "pred_h" : "SYS_sample_data_senml.csv",
	        "stat_h" : "SYS_sample_data_senml.csv",
	        "train_h" : "inputFileForTimerSpout-CITY.csv"
	     }

property_files = {
		    "etl_l" : "etl_topology.properties",
		    "pred_l" : "tasks_CITY.properties",
		    "stat_l" : "stats_with_vis_topo.properties",
		    "train_l" : "iot_train_topo_city.properties",
		    "etl_h" : "etl_topology.properties",
		    "pred_h" : "tasks_CITY.properties",
		    "stat_h" : "stats_with_vis_topo.properties",
		    "train_h" : "iot_train_topo_city.properties"
		 }
		 
topo_qualified_path = {
			"etl_l" : "in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology",
			"pred_l" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTPredictionTopologySYS",
			"stat_l" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTStatsTopology",
			"train_l" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTTrainTopologySYS",
			"etl_h" : "in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology",
			"pred_h" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTPredictionTopologySYS",
			"stat_h" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTStatsTopology",
			"train_h" : "in.dream_lab.bm.stream_iot.storm.topo.apps.IoTTrainTopologySYS"
			}
			
paths={'buzz':'/home/fuxinwei/EdgeWiseATC19/EdgeWise-ATC-19-Benchmarks/scirpts'}
results_dir={'buzz':'/home/fuxinwei/EdgeWiseATC19/EdgeWise-ATC-19-Benchmarks/scirpts/experiment_results'}

pi_outdir = "/home/pi/topo_run_outdir/reg-SYS"

PORT = 38999
