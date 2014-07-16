package com.ym.storm;

import java.util.Queue;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.log4j.Logger;

public class GenArrival implements Runnable {
	private static Logger log = Logger.getLogger(GenArrival.class);
	private Queue<String> m_files;
	private Queue<VideoSlice> m_Vqueue;
	private long m_maxRun;
	private int maxThread;
	private long m_jobIndex;
	private double m_initDelay;
	private VideoSpout m_vs;
	private ExponentialDistribution m_expDis;
	private String m_codingSpeed;
	
	public GenArrival(VideoSpout vs, double initDelay, double mean, long maxRun, 
			Queue<String> files, Queue<VideoSlice> Vqueue, String codingSpeed, int maxThread){
		m_initDelay = initDelay;
		m_vs = vs;
		m_files = files;
		m_Vqueue = Vqueue;
		m_maxRun = maxRun;
		m_jobIndex = 0;
		m_expDis = new ExponentialDistribution(mean);
		m_codingSpeed = codingSpeed;
		this.maxThread = maxThread;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try  
        {
			Thread.currentThread().sleep((long)(1000*m_initDelay)); // init sleep
			while(m_maxRun-- > 0){
				VideoSlice v_slice = new VideoSlice();
				String filename = m_files.poll();
				v_slice.setInputFileName(filename); // get a file
				v_slice.setInputFilePath(m_vs.getInputDir());
				v_slice.setJobIndex(m_vs.getNextJobIndex());
				v_slice.setCodingSpeed(m_codingSpeed);
				v_slice.setMaxThread(maxThread);
				
				m_files.add(filename); // always
				m_Vqueue.add(v_slice); // 
				log.info("[ym-log] generate "+v_slice.getJobIndex()+" "+ v_slice.getInputFileName());
//				System.out.println("Thread1 sold " + m_jobIndex++);
				Thread.currentThread().sleep((long) (1000*m_expDis.sample()+1)); // interval  
//				Thread.currentThread().sleep(1000); // interval
			}            
        }  
        catch(Exception e)  
        {  
            System.out.println(e.toString());  
        }  
          
          
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		GenArrival gen = new GenArrival(10);
//		gen.run();
//		Thread a = new Thread(new GenArrival(10));
//		a.setDaemon(true);
//		a.start();
		
		ExponentialDistribution x = new ExponentialDistribution(10000-1);
		System.out.println("mean="+x.getNumericalMean());
		System.out.println("var="+x.getNumericalVariance());
		System.out.println("low="+x.getSupportLowerBound());
		System.out.println("up="+x.getSupportUpperBound());
		for (int i=0; i< 10; i++){
			System.out.println((long)(x.sample())+1);
		}
	}
}
