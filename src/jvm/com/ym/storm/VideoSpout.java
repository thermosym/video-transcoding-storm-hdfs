package com.ym.storm;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class VideoSpout implements IRichSpout{

	public VideoSpout(String inputDir,String hdfsServer, double mean_arrival, String codingSpeed, int maxRun, int maxThread) {
		super();
		// TODO Auto-generated constructor stub
		m_inputDir = inputDir;
		m_hdfsServer = hdfsServer;
		this.maxRun = maxRun;
		this.maxThread = maxThread;
		jobIndex = 0;
		m_mean_arrival = mean_arrival;
		m_codingSpeed = codingSpeed;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 8032868069400489316L;
	private static Logger log = Logger.getLogger(VideoSpout.class);
	private SpoutOutputCollector m_collector;
    private Map m_conf;
    private TopologyContext m_context;
    private FileSystem m_fs;
    private String m_inputDir;
    private Queue<String> m_files;
    private Queue<VideoSlice> m_Vqueue;
    private String m_hdfsServer;
    private long maxRun;
    private int maxThread;
    private long jobIndex;
    private double m_mean_arrival;
    private String m_codingSpeed;
    
    private void scanNewFiles (String path){
    	m_files = new LinkedList<String>(HDFSUtil.listFile(m_fs, m_inputDir));
    }
    
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		// init something of storm
		m_conf = conf;
		m_context = context;
		m_collector = collector;
		// init the hadoop file system
		m_fs = HDFSUtil.getFileSystem(m_hdfsServer, 9000);
		
		// scan the files in the folder
		scanNewFiles(m_inputDir);
		if (m_files.size() == 0){
			log.info("no files in the dir: " + m_inputDir);
		}
		
		//init queue
		m_Vqueue = new LinkedList<VideoSlice>();
		
		// create thread to generate video arrival
		Thread gen_th = new Thread(new GenArrival(this, 5, m_mean_arrival, maxRun, m_files, m_Vqueue, m_codingSpeed, maxThread));
		gen_th.setDaemon(true);
		gen_th.start();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			m_fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(m_Vqueue.isEmpty()){
			return;
		}else{
			
			//v_slice.setResolution("352x288"); //352x288 1280x768
			VideoSlice v_slice = m_Vqueue.poll();
			log.info("[ym-log] emit "+v_slice.getJobIndex()+" "+ v_slice.getInputFileName());
			m_collector.emit(new Values(v_slice), v_slice);
			//TODO: remove it when run in real-data
			
		}
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		// remove files from the
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		// re-enqueue the files
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//declarer.declare(new Fields("video_filename"));
//		List<String> fieldsArr = new ArrayList<String>();
//		fieldsArr.add("videofile");
//		fieldsArr.add("resolution");
		
		declarer.declare(new Fields("videofile"));  
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getNextJobIndex() {
		return jobIndex++;
	}

	public String getInputDir(){
		return m_inputDir;
	}
}
