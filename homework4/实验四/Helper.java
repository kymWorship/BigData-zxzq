import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;

public class Helper {
	// 删除文件或目录
	public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
		if(isDeleteDir) pathStr = pathStr.substring(0, pathStr.lastIndexOf('/'));
		Path path = new Path(pathStr);
		Configuration configuration = new Configuration();
		// 获取 HDFS 文件系统
		FileSystem fileSystem = path.getFileSystem(configuration);
		if(fileSystem.exists(path)) fileSystem.delete(path, true);
	}
	
	// 将一个文件的内容拷贝到另一个文件中
	public static void copyFile(String from_path, String to_path) throws IOException {
		Path path_from = new Path(from_path);
		Path path_to = new Path(to_path);
		Configuration configuration = new Configuration();
		// 获取 HDFS 文件系统
		FileSystem fileSystem = FileSystem.get(configuration);
		FSDataInputStream inputStream = fileSystem.open(path_from);
		LineReader lineReader = new LineReader(inputStream, configuration);
		FSDataOutputStream outputStream = fileSystem.create(path_to);
		Text line = new Text();
		while(lineReader.readLine(line) > 0) {
			String str = line.toString() + "\n";
			outputStream.write(str.getBytes());
		}
		lineReader.close();
		outputStream.close();
	}
	
	// 从文件系统中读取中心点集
	public static ArrayList<ArrayList<Float>> getCenters(String filePath) throws IOException {
		Path path = new Path(filePath);
		Configuration conf = new Configuration();
		ArrayList<ArrayList<Float>> centers = new ArrayList<ArrayList<Float>>();	// 存储读取的中心点集
		// 获取 HDFS 文件系统
		FileSystem fileSystem = FileSystem.get(conf);
		FSDataInputStream inputStream = fileSystem.open(path);
		LineReader lineReader = new LineReader(inputStream, conf);	// 一行一行读文件
		Text line = new Text();
		while(lineReader.readLine(line) > 0) {
			String[] features = line.toString().split("\\s+")[1].split(",");	// 获取该点各维数值
			ArrayList<Float> center = new ArrayList<Float>();	// 用于存储该点
			for(String feature : features) center.add(Float.parseFloat(feature));	// 类型转化
			centers.add(center);	// 存到点集中
		}
		lineReader.close();
		return centers;
	}
	
	// 判断是否收敛
	public static boolean isConverged (String oldPath, String newPath) throws IOException {
		ArrayList<ArrayList<Float>> oldCenters = Helper.getCenters(oldPath);
		ArrayList<ArrayList<Float>> newCenters = Helper.getCenters(newPath);
		if(oldCenters.size() != newCenters.size()) return false;
		for(int i = 0; i < oldCenters.size(); ++i) {
			if(oldCenters.get(i).size() != newCenters.get(i).size()) return false;
			for(int j = 0; j < oldCenters.get(i).size(); ++j) { 
				if(Math.abs(oldCenters.get(i).get(j) - newCenters.get(i).get(j)) > 1e-6) return false;
			}
		}
		return true;
	}
	
	// 用于将数组存储的点转化为字符串(格式："f1,f2,f3,...,f10")
	public static String ArrayList2String(ArrayList<Float> point) {
		String res = point.get(0).toString();
		for(int i = 1; i < point.size(); ++i) res += "," + point.get(i).toString();
		return res;
	}
}
