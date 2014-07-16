package com.ym.storm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;



public class RunCommand {
	//private static Log log = LogFactory.getLog(RunCommand.class.getName());
	private static Logger logger = org.apache.log4j.Logger.getLogger(RunCommand.class);
		    
	public static List<String> getPhysicalAddress() {
		Process p = null;
		List<String> address = new ArrayList<String>();//
		try {
			p = new ProcessBuilder("ifconfig", "-a").start();// 
		} catch (IOException e) {
			return address;
		}

		byte[] b = new byte[1024];
		int readbytes = -1;
		StringBuffer sb = new StringBuffer();

		InputStream in = p.getInputStream();
		try {
			while ((readbytes = in.read(b)) != -1) {
				sb.append(new String(b, 0, readbytes));
			}
		} catch (IOException e1) {
			logger.error(ExceptionUtils.getFullStackTrace(e1));
		} finally {
			try {
				in.close();
			} catch (IOException e2) {
				logger.error(ExceptionUtils.getFullStackTrace(e2));
			}
		}

		String rtValue = sb.toString();
		int i = rtValue.indexOf("HWaddr");
		while (i > 0) {
			rtValue = rtValue.substring(i + "HWaddr".length());
			address.add(rtValue.substring(1, 18));
			i = rtValue.indexOf("HWaddr");
		}
		return address;

	}

	public static void printSystemEnv(ProcessBuilder pb) {
		Map<String, String> env = pb.environment();// 
		Iterator<String> it = env.keySet().iterator();
		String sysatt = null;
		while (it.hasNext()) {
			sysatt = (String) it.next();
			System.out.println("System Attribute:" + sysatt + "="
					+ env.get(sysatt));
		}
		System.out.println("===============");
	}

	/**
	 * @param working_dir set the working dir of the command
	 * @param command List of command parameters
	 * @return true
	 */
	public static boolean executeCommand(File working_dir, List<String> command) {
		int status = 0;
		ProcessBuilder pb = new ProcessBuilder(command);
		pb.directory(working_dir); // set the running dir
		Process p = null;
		try {
			logger.info( ( new StringBuffer("Run process:")
				.append(working_dir).append("$")
				.append(command.toString()) ).toString() );
			p = pb.redirectErrorStream(true).start();
//			new PrintStream(p.getInputStream()).start();
			InputStream stdin = p.getInputStream();// get stdout
			// wait for the process
			status = p.waitFor(); 
						 
			byte[] b = new byte[1024];
			int readbytes = -1;
			StringBuffer sb = new StringBuffer();
			// print stdout
			while ((readbytes = stdin.read(b)) != -1) {
				sb.append(new String(b, 0, readbytes));
			}
			stdin.close();
			
			if (status != 0){ // see error
				StringBuffer err_info = new StringBuffer("error on running command: ")
					.append(working_dir).append("$")
					.append(command.toString())
					.append("\n*---error-info---*\n").append(sb)
					.append("\n*----------------*");
				logger.error(err_info);
			}else{
				logger.info("success on running command");
			} // successful return 0
		
		} catch (Exception e){
			logger.error(ExceptionUtils.getFullStackTrace(e));
		}
		
		return (0 == status);
	}


	public static List<String> encodingCommandBuilder(String speed, String inputFile, String outputFile) {
		List<String> command = new ArrayList<String>();
		// ffmpeg -y -s 1280x768 -i snowwhite.yuv -vcodec mpeg4 -r 30 -qscale 6 -pix_fmt yuv420p snow.mp4
		command.add("/usr/local/ffmpeg/bin/ffmpeg");
		command.add("-y");
		command.add("-i");
		command.add(inputFile);
		command.add("-vcodec");
		command.add("libx264");
		command.add("-preset");
		command.add(speed);
		command.add("-crf");
		command.add("20");
		command.add(outputFile);
		
		return command;
	}

	public static List<String> ffmpegCommandBuilder(String speed,
			String inputFile, String outputFile, int maxThreads) {
		List<String> command = new ArrayList<String>();
		// ffmpeg -y -s 1280x768 -i snowwhite.yuv -vcodec mpeg4 -r 30 -qscale 6
		// -pix_fmt yuv420p snow.mp4
		command.add("/usr/local/ffmpeg/bin/ffmpeg");
		command.add("-y");
		command.add("-i");
		command.add(inputFile);
		command.add("-vcodec");
		command.add("libx264");
		command.add("-preset");
		command.add(speed);
		command.add("-crf");
		command.add("20");
		if (maxThreads > 0){
			command.add("-threads");
			command.add(String.valueOf(maxThreads));	
		}		
		command.add(outputFile);

		return command;
	}

	public static List<String> x264CommandBuilder(String speed,
			String resolution, String inputFile, String outputFile) {
		List<String> command = new ArrayList<String>();
		// 'x264 bbb_%i.yuv --input-res 1920x1080 --fps 24 --crf 18 --preset
		// medium -o bbb_%i.avi'%(i,i)
		command.add("/usr/local/bin/x264");
		command.add(inputFile);
		command.add("--input-res");
		command.add(resolution);
		command.add("--fps");
		command.add("24");
		command.add("--crf");
		command.add("18");
		command.add("--preset");
		command.add(speed);
		command.add("-o");
		command.add(outputFile);

		return command;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// List<String> address = RunCommand.getPhysicalAddress();
		// for(String add : address){
		// System.out.printf("Physic address: %s\n",add);
		// }
		// System.out.printf("\n------------\n");

		List<String> command = encodingCommandBuilder(
				"fast",
				"snowwhite.yuv",
				"snowwhite.mp4");

		boolean status = executeCommand(new File("/home/ym/Videos"), command);
		
	}

}