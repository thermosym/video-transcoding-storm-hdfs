package com.ym.storm;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ThreadBenchBolt implements IRichBolt {
	private static final long serialVersionUID = -6161188759112346222L;
	private static Logger log = Logger.getLogger(ThreadBenchBolt.class);
	
	private OutputCollector m_collector;
    private Map m_conf;
    private TopologyContext m_context;
    private FileSystem m_fs;
	private String m_outputDir;

	 
	public ThreadBenchBolt(String outputDir) {
		super();
		this.m_outputDir = outputDir;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		m_conf = stormConf;
		m_context = context;
		m_collector = collector;
		// init the hadoop file system		
	}

	private String createTempDir(String rootDir){
		//get random dir name
		UUID temp_dirID = UUID.randomUUID();
		File dirFile  = null ;
        try {
           dirFile = new File(rootDir +"/"+ temp_dirID.toString());
            if ( !(dirFile.exists()) && !(dirFile.isDirectory())) {
                boolean  creadok  =  dirFile.mkdirs();
                if (creadok) {
                   log.debug("create dir success:"+dirFile.toString());
               } else {
            	   log.debug("fail to create dir:"+dirFile.toString());            
               } 
           }
        } catch (Exception e) {
        	log.error(ExceptionUtils.getFullStackTrace(e));
            return "";
       }
       return dirFile.toString();
	}
	
	private void removeTempDir(String dir){
		File folder = new File(dir);
		try {
			String childs[] = folder.list();
			if ( childs.length > 0) {
				for (int i = 0; i < childs.length; i++) {
					String childName = childs[i];
					String childPath = folder.getPath() + File.separator
							+ childName;
					File filePath = new File(childPath);
					if (filePath.isFile()) {
						// delete child file
						filePath.delete();
					} else{
						// delete child dir
						removeTempDir(filePath.toString());
					}
				}
			}
			// delete parent dir
			folder.delete();
		} catch (Exception e) {
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		// get temp dir
		String tmpDir = createTempDir("/tmp");
		
		// get the video file from HDFS
		VideoSlice inputVideo = (VideoSlice) input.getValueByField("videofile");
		String remoteFile = inputVideo.getInputFilePath()+File.separator+inputVideo.getInputFileName();
		//log.info("[ym-log] start-task "+tmpDir+ " "+inputVideo.getInputFileName());
		log.info("[ym-log] start-task "+inputVideo.getJobIndex()+ " " +inputVideo.getInputFileName());
		//log.info("[ym-log] start-download-video " +tmpDir+ " "+inputVideo.getInputFileName());
		
		String filelocation = HDFSUtil.getLocation(m_fs, remoteFile);
		
		long fileSize_org = HDFSUtil.download(m_fs, tmpDir, remoteFile);
		
		log.info("[ym-log] end-download-video " +inputVideo.getJobIndex()+ " "+inputVideo.getInputFileName()+
				" size: "+ fileSize_org + " location: "+filelocation);
		// encode it
//		String outputFile = inputVideo.getInputFileName().substring(
//				0, inputVideo.getInputFileName().lastIndexOf(".")) + ".mp4";
		String outputFile = inputVideo.getInputFileName().substring(
				0, inputVideo.getInputFileName().lastIndexOf(".")) +"."+UUID.randomUUID().toString()+ ".mp4";
		
//		List<String> command = RunCommand.encodingCommandBuilder(
//				inputVideo.getCodingSpeed(),
//				inputVideo.getInputFileName(),
//				outputFile);
//		List<String> command = RunCommand.ffmpegCommandBuilder(
//				inputVideo.getCodingSpeed(),inputVideo.getInputFileName(), outputFile);
		List<String> command = RunCommand.x264CommandBuilder(
				inputVideo.getCodingSpeed(), "1920x1080", inputVideo.getInputFileName(), outputFile);
	
		//log.info("[ym-log] start-encode " + tmpDir+ " "+inputVideo.getInputFileName());
		log.info("[ym-log] start-encode " + inputVideo.getJobIndex() + " "+inputVideo.getInputFileName());
		RunCommand.executeCommand(new File(tmpDir), command);
		//log.info("[ym-log] end-encode " + tmpDir+ " "+inputVideo.getInputFileName());
		log.info("[ym-log] end-encode " + inputVideo.getJobIndex() + " "+inputVideo.getInputFileName());
		
		
		// upload files
		//log.info("[ym-log] start-upload " + tmpDir);
		log.info("[ym-log] start-upload " + inputVideo.getJobIndex());
		long fileSize_out = HDFSUtil.upload(m_fs, tmpDir+File.separator+outputFile,
				m_outputDir+File.separator+outputFile);
		//log.info("[ym-log] end-upload " + tmpDir);
		log.info("[ym-log] end-upload " + inputVideo.getJobIndex() + " size: " + fileSize_out);
		log.info("upload it to dir:" + m_outputDir+File.separator+outputFile);
		
		// remove the temp files
		removeTempDir(tmpDir);
		log.info("delete the temp files");
		
		m_collector.ack(input); //ack it
		//log.info("[ym-log] ack-tuple " + tmpDir);
		log.info("[ym-log] ack-tuple " + inputVideo.getJobIndex());
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			m_fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
