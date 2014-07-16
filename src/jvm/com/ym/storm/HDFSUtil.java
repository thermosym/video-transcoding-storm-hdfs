package com.ym.storm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;

public class HDFSUtil {
	private static Logger logger = org.apache.log4j.Logger
			.getLogger(HDFSUtil.class);

	public synchronized static FileSystem getFileSystem(String ip, int port) {
		FileSystem fs = null;
		String url = "hdfs://" + ip + ":" + String.valueOf(port);
		Configuration config = new Configuration();
		config.set("fs.default.name", url);
		try {
			fs = FileSystem.get(config);
		} catch (Exception e) {
			logger.error("getFileSystem failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
		return fs;
	}

	public synchronized static void listNode(FileSystem fs) {
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		try {
			DatanodeInfo[] infos = dfs.getDataNodeStats();
			for (DatanodeInfo node : infos) {
				System.out.println("HostName: " + node.getHostName() + "/n"
						+ node.getDatanodeReport());
				System.out.println("--------------------------------");
			}
		} catch (Exception e) {
			logger.error("list node list failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * 打印系统配置
	 * 
	 * @param fs
	 */
	public synchronized static void listConfig(FileSystem fs) {
		Iterator<Entry<String, String>> entrys = fs.getConf().iterator();
		while (entrys.hasNext()) {
			Entry<String, String> item = entrys.next();
			logger.info(item.getKey() + ": " + item.getValue());
		}
	}

	/**
	 * 创建目录和父目录
	 * 
	 * @param fs
	 * @param dirName
	 */
	public synchronized static void mkdirs(FileSystem fs, String dirName) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		String dir = workDir + "/" + dirName;
		Path src = new Path(dir);
		// FsPermission p = FsPermission.getDefault();
		boolean succ;
		try {
			succ = fs.mkdirs(src);
			if (succ) {
				logger.info("create directory " + dir + " successed. ");
			} else {
				logger.info("create directory " + dir + " failed. ");
			}
		} catch (Exception e) {
			logger.error("create directory " + dir + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * 删除目录和子目录
	 * 
	 * @param fs
	 * @param dirName
	 */
	public synchronized static void rmdirs(FileSystem fs, String dirName) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		String dir = workDir + "/" + dirName;
		Path src = new Path(dir);
		boolean succ;
		try {
			succ = fs.delete(src, true);
			if (succ) {
				logger.info("remove directory " + dir + " successed. ");
			} else {
				logger.info("remove directory " + dir + " failed. ");
			}
		} catch (Exception e) {
			logger.error("remove directory " + dir + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	/**
	 * 上传目录或文件
	 * 
	 * @param fs
	 * @param local
	 * @param remote
	 */
	public synchronized static long upload(FileSystem fs, String local,
			String remote) {
		long fileSize=0;
		//Path home = fs.getHomeDirectory();
		//Path workDir = fs.getWorkingDirectory();
		//Path dst = new Path(workDir + "/" + remote);
		Path dst = new Path(remote);
		Path src = new Path(local);
		try {
			fs.copyFromLocalFile(false, true, src, dst);
			
			FileStatus fstat = fs.getFileStatus(dst);
			fileSize = fstat.getLen();
			
			logger.info("upload " + local + " to  " + remote + " successed. ");
		} catch (Exception e) {
			logger.error("upload " + local + " to  " + remote + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
		return fileSize;
	}

	/**
	 * 下载目录或文件
	 * 
	 * @param fs
	 * @param local
	 * @param remote
	 */
	public synchronized static long download(FileSystem fs, String local,
			String remote) {
		long fileSize=0;
		//Path home = fs.getHomeDirectory();
		//Path workDir = fs.getWorkingDirectory();
		//Path dst = new Path(workDir + "/" + remote);
		//Path dst = new Path(home + "/" + remote);
		Path src = new Path(remote);
		Path dst = new Path(local);
		try {
			FileStatus fstat = fs.getFileStatus(src);
//			BlockLocation[] blkLocations  = fs.getFileBlockLocations(fstat, 0, fstat.getLen());
			fileSize = fstat.getLen();
			fs.copyToLocalFile(false, src, dst);
			
//			StringBuffer locationInfo = new StringBuffer();
//			for(String host: blkLocations[0].getHosts()){
//				locationInfo.append(host).append(" ");
//			}
			logger.info("download from " + remote + " to  " + local);
//					+ " successed. block: " + locationInfo);
		} catch (Exception e) {
			logger.error("download from " + remote + " to  " + local
					+ " failed :" + ExceptionUtils.getFullStackTrace(e));
		}
		return fileSize;
	}

	public synchronized static String getLocation(FileSystem fs,
			String remote) {
		Path dst = new Path(remote);
		StringBuffer locationInfo = new StringBuffer();;
		try {
			FileStatus fstat = fs.getFileStatus(dst);
			BlockLocation[] blkLocations  = fs.getFileBlockLocations(fstat, 0, fstat.getLen());
			
			for(String host: blkLocations[0].getHosts()){
				locationInfo.append(host).append(" ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return locationInfo.toString();
	}
	
	/**
	 * 字节数转换
	 * 
	 * @param size
	 * @return
	 */
	public synchronized static String convertSize(long size) {
		String result = String.valueOf(size);
		if (size < 1024 * 1024) {
			result = String.valueOf(size / 1024) + " KB";
		} else if (size >= 1024 * 1024 && size < 1024 * 1024 * 1024) {
			result = String.valueOf(size / 1024 / 1024) + " MB";
		} else if (size >= 1024 * 1024 * 1024) {
			result = String.valueOf(size / 1024 / 1024 / 1024) + " GB";
		} else {
			result = result + " B";
		}
		return result;
	}

	/**
	 * 遍历HDFS上的文件和目录
	 * 
	 * @param fs
	 * @param path
	 */
	public synchronized static List<String> listFile(FileSystem fs, String path) {
		List<String> file_list = new ArrayList<String>();
		
		Path workDir = fs.getWorkingDirectory();
		Path dst;
		if (null == path || "".equals(path)) {
			dst = new Path(workDir + "/" + path);
		} else {
			dst = new Path(path);
		}
		
		try {
			String relativePath = "";
			FileStatus[] fList = fs.listStatus(dst);
			for (FileStatus f : fList) {
				if (null != f) {
					relativePath = f.getPath().getName();
							// new StringBuffer()
							//.append(f.getPath().getParent()).append("/")
							//.append(f.getPath().getName()).toString();
					if (f.isDir()) {
						//listFile(fs, relativePath);
						//TODO: recursive list dir
					} else {
						//System.out.println(convertSize(f.getLen()) + "\t\t" + relativePath);
						file_list.add(relativePath);
					}
				}
			}
		} catch (Exception e) {
			logger.error("list files of " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
		}
		
		return file_list;
	}

	public synchronized static void write(FileSystem fs, String path,
			String data) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			FSDataOutputStream dos = fs.create(dst);
			dos.writeUTF(data);
			//dos.write
			dos.close();
			logger.info("write content to " + path + " successed. ");
		} catch (Exception e) {
			logger.error("write content to " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	public synchronized static void append(FileSystem fs, String path,
			String data) {
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			FSDataOutputStream dos = fs.append(dst);
			dos.writeUTF(data);
			dos.close();
			logger.info("append content to " + path + " successed. ");
		} catch (Exception e) {
			logger.error("append content to " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
	}

	public synchronized static String read(FileSystem fs, String path) {
		String content = null;
		// Path home = fs.getHomeDirectory();
		Path workDir = fs.getWorkingDirectory();
		Path dst = new Path(workDir + "/" + path);
		try {
			// reading
			FSDataInputStream dis = fs.open(dst);
			content = dis.readUTF();
			dis.close();
			logger.info("read content from " + path + " successed. ");
		} catch (Exception e) {
			logger.error("read content from " + path + " failed :"
					+ ExceptionUtils.getFullStackTrace(e));
		}
		return content;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		FileSystem fs = HDFSUtil.getFileSystem("localhost", 9000);
		//HDFSUtil.listConfig(fs);
		HDFSUtil.listNode(fs);
		List<String> files = HDFSUtil.listFile(fs, "/user");
		for (String file: files) {
			System.out.println(file);
		}
		//String dirName = "/user/ym/demo.txt";
		//HDFSUtil.write(fs, dirName, "test-1");
		// FsUtil.mkdirs(fs, dirName);
		// FsUtil.rmdirs(fs, dirName);
		//HDFSUtil.download(fs, "/home/ym/Downloads/demo.txt", dirName);
		//HDFSUtil.upload(fs, "/home/ym/zookeeper-3.4.5.tar.gz", "/user/");
		//HDFSUtil.download(fs, "/home/ym/Downloads/", "/user/zookeeper-3.4.5.tar.gz");
		
		
		//HDFSUtil.append(fs, dirName, "/ntest-2");
		//
		// String content = FsUtil.read(fs, dirName);
		//
		// System.out.println(content);
		// FsUtil.listFile(fs, "");
		// FsUtil.listNode(fs);
		fs.close();
	}

}
