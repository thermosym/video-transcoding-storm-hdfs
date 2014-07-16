package com.ym.storm;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class ThreadBench {
	private static Logger log = Logger.getLogger(ThreadBench.class);
	public ThreadBench() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		//String hdfs_server = "172.18.0.26";
		String hdfs_server = "155.69.151.149";
		double mean_arrival = 1.5;
		String input_dir = "/yangming/input";
		String output_dir = "/yangming/output";
		String codingSpeed = "fast";
		int maxRun = 200;
		
		if (args.length != 6){
			log.error("args not sufficient, [topology_name][hdfs_server][mean_arrival][input_dir][output_dir][codingSpeed]\n" +
					"eg: myTopology 172.18.0.26 1 /user/ym/input /user/ym/output fast");
			return;
		}else{
			log.info("agrs are: " + args[1] + " " + args[2] + " " + args[3] + " "+  args[4]
					+ " " + args[5]);
			hdfs_server = args[1];
			mean_arrival = Double.parseDouble(args[2]) ;
			input_dir = args[3];
			output_dir = args[4];
			codingSpeed = args[5];
		}
		
		try {
            TopologyBuilder topologyBuilder = new TopologyBuilder(); 
            topologyBuilder.setSpout("VSpout", new VideoSpout(input_dir,hdfs_server,mean_arrival,codingSpeed,maxRun, 1), 1);
            topologyBuilder.setBolt("encodeBolt", new VideoEncodeBolt(output_dir,hdfs_server), 8).shuffleGrouping("VSpout");
            Config config = new Config();
            config.setMessageTimeoutSecs(12000);
            Config.setMaxSpoutPending(config, 100);
            
            //config.setDebug(true);
            if (args != null && args.length > 0) {
                config.setNumWorkers(14);
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } else {
                // local mode
                config.setMaxTaskParallelism(10);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("test", config, topologyBuilder.createTopology());
            }
            
        } catch (Exception e) {
        	log.error(ExceptionUtils.getFullStackTrace(e));
        }

	}

}
