package com.ym.storm;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class CodingJob {
	private static Logger log = Logger.getLogger(CodingJob.class);
	
	public static void main(String[] args) {
		
		String hdfs_server = "172.18.0.26";
//		String hdfs_server = "155.69.151.149";
		int workerNumber = 2;
		int taskNumber = 10;
		double mean_arrival = 1.5;
		String input_dir = "/yangming/input";
		String output_dir = "/yangming/output";
		String codingSpeed = "fast";
		int maxRun = 200;
		int maxThread = 0; // o:not limit
		
		if (args.length < 10){
			log.error("args not sufficient, [topology_name][hdfs_server][workerNumber][taskNumber][mean_arrival][input_dir][output_dir][codingSpeed][maxRun][Threads]\n" +
					"eg: myTopology 172.18.0.26 2 10 1.0 /yangming/input /yangming/output fast 200 1");
			return;
		}else{
			log.info("agrs are: " + args[1] + " " + args[2] + " " + args[3] + " "+  args[4]
					+ " " + args[5] + " " + args[6] + " " + args[7]);
			hdfs_server = args[1];
			workerNumber = Integer.parseInt(args[2]);
			taskNumber = Integer.parseInt(args[3]);
			mean_arrival = Double.parseDouble(args[4]) ;
			input_dir = args[5];
			output_dir = args[6];
			codingSpeed = args[7];
			maxRun = Integer.parseInt(args[8]);
			maxThread = Integer.parseInt(args[9]);
		}
		
		try {
            TopologyBuilder topologyBuilder = new TopologyBuilder(); 
            topologyBuilder.setSpout("VSpout", new VideoSpout(input_dir,hdfs_server,mean_arrival,codingSpeed, maxRun, maxThread), 1);
            topologyBuilder.setBolt("encodeBolt", new VideoEncodeBolt(output_dir,hdfs_server), taskNumber)
            	.setNumTasks(taskNumber).shuffleGrouping("VSpout");
            Config config = new Config();
            config.setMessageTimeoutSecs(12000);
            Config.setMaxSpoutPending(config, 100);
            
            //config.setDebug(true);
            if (args != null && args.length > 0) {
                config.setNumWorkers(workerNumber);
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } else {
                // local mode
                config.setMaxTaskParallelism(10); // only used in local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("test", config, topologyBuilder.createTopology());
            }
            
        } catch (Exception e) {
        	log.error(ExceptionUtils.getFullStackTrace(e));
        }
	}

}
