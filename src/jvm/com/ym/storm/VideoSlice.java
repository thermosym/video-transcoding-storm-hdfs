package com.ym.storm;

import java.io.Serializable;

public class VideoSlice implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2050780436253291027L;

	private String inputFileName;
	private String inputFilePath;
	private String resolution;
	private String codingSpeed;
	private long jobIndex;
	private int maxThread;
	
	public VideoSlice() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public String getInputFileName() {
		return inputFileName;
	}
	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}

	public String getResolution() {
		return resolution;
	}
	public void setResolution(String resolution) {
		this.resolution = resolution;
	}

	public String getInputFilePath() {
		return inputFilePath;
	}

	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	public long getJobIndex() {
		return jobIndex;
	}

	public void setJobIndex(long jobIndex) {
		this.jobIndex = jobIndex;
	}

	public String getCodingSpeed() {
		return codingSpeed;
	}

	public void setCodingSpeed(String codingSpeed) {
		this.codingSpeed = codingSpeed;
	}

	public int getMaxThread() {
		return maxThread;
	}

	public void setMaxThread(int maxThread) {
		this.maxThread = maxThread;
	}

}
