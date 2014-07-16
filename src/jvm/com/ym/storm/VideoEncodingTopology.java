package com.ym.storm;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class VideoEncodingTopology {
	
	private static Logger log = Logger.getLogger(VideoEncodeBolt.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String hdfs_server = "172.18.0.26";
		String input_dir = "/user/ym/input";
		String output_dir = "/user/ym/output";
		
		
		if (args.length != 4){
			log.error("args not sufficient, [topology_name][hdfs_server][input_dir][output_dir]\n" +
					"eg: myTopology 172.18.0.26 /user/ym/input /user/ym/output");
		}else{
			log.info("agrs are: " + args[1] + " " + args[2] + " " + args[3]);
			hdfs_server = args[1];
			input_dir = args[2];
			output_dir = args[3];
		}
		
		try {
            TopologyBuilder topologyBuilder = new TopologyBuilder(); 
            topologyBuilder.setSpout("VSpout", new VideoSpout(input_dir,hdfs_server,1, "fast", 200, 1), 1);
            topologyBuilder.setBolt("encodeBolt", new VideoEncodeBolt(output_dir,hdfs_server), 80).shuffleGrouping("VSpout");
            Config config = new Config();
            config.setMessageTimeoutSecs(120);
            Config.setMaxSpoutPending(config, 200);
            
            //config.setDebug(true);
            if (args != null && args.length > 0) {
                config.setNumWorkers(80);
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
