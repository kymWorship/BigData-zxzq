import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {
	/* 自定义 Mapper */
	public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private ArrayList<ArrayList<Float>> centers = new ArrayList<ArrayList<Float>>();	// 用于存储本轮的中心点
		private ArrayList<Integer> clusterIDs = new ArrayList<Integer>();	// 用于存储中心点的ClusterID，防止输入文件中ID乱序的情况
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {	// 从DistributedCache中读取当前的中心点集
			URI centersFileUri = context.getCacheFiles()[0];	// 获取储存中心点集的文件路径
			String centersFileName = new Path(centersFileUri.getPath()).getName().toString();	// 获取储存中心点集的文件名
			BufferedReader centersReader = new BufferedReader(new FileReader(centersFileName));	// 创建文件读取器
			
			String line;	// 文件中的一行  
			while((line = centersReader.readLine()) != null) {
				String id = line.split("\\s+")[0];
				String[] features = line.split("\\s+")[1].split(",");	// 获取该点各维数值
				ArrayList<Float> center = new ArrayList<Float>();	// 用于存储该点
				for(String feature : features) center.add(Float.parseFloat(feature));	// 类型转化
				assert(center.size() == 10);
				centers.add(center);	// 存到点集中
				clusterIDs.add(Integer.parseInt(id));	// 存到ID集中
			}
			centersReader.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] features = value.toString().split("\\s+");	// ID + 10维数据
			ArrayList<Float> point = new ArrayList<Float>();
			for(int i = 1; i < features.length; ++i) point.add(Float.parseFloat(features[i]));	// 10维数据
			assert(point.size() == 10);
			
			Float minDis = Float.MAX_VALUE;
			int index = -1;
			for(int i = 0; i < centers.size(); ++i) {
				float dis2 = 0f;	// 到当前中心点的欧氏距离的平方
				for(int j = 0; j < point.size(); ++j) dis2 += Math.pow(point.get(j) - centers.get(i).get(j), 2);	// 计算距离的平方
				if(dis2 < minDis) {	// 如果更小则更新
					minDis = dis2;
					index = i;
				}
			}
			// key -> 类ID, value -> 当前点的10维数据#1 (f1,f2,...#1)
			context.write(new IntWritable(clusterIDs.get(index)), new Text(Helper.ArrayList2String(point) + "#1"));
		}
	}
	
	/* 自定义 Combiner */
	public static class KmeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override 
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* 用于将同一个Worker产生的数据聚合，减少通讯开销 */
			ArrayList<Float> pm = new ArrayList<Float>(Collections.nCopies(10, 0f));	// 同簇点的均值
			assert(pm.size() == 10);
			
			Integer tot = 0;	// 同簇点的数量
			for(Text value : values) {	// 计算同簇点的和
				String[] features = value.toString().split("#")[0].split(",");
				assert(features.length == 10);
				Integer num = Integer.parseInt(value.toString().split("#")[1]);
				for(int i = 0; i < pm.size(); ++i) pm.set(i, pm.get(i) + Float.parseFloat(features[i]) * num);
				tot += num;
			}
			for(int i = 0; i < pm.size(); ++i) pm.set(i, pm.get(i) / tot);	// 计算同簇点的均值
			
			// key -> 类ID, value -> 当前点的10维数据#tot (f1,f2,...#tot)
			context.write(key, new Text(Helper.ArrayList2String(pm) + "#" + tot.toString()));	
		}
	}
	
	/* 自定义 Reducer */
	public static class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override 
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* 用于将同一个Worker产生的数据聚合，减少通讯开销 */
			ArrayList<Float> pm = new ArrayList<Float>(Collections.nCopies(10, 0f));	// 同簇点的均值
			assert(pm.size() == 10);
			
			Integer tot = 0;	// 同簇点的数量
			for(Text value : values) {	// 计算同簇点的和
				String[] features = value.toString().split("#")[0].split(",");
				assert(features.length == 10);
				Integer num = Integer.parseInt(value.toString().split("#")[1]);
				for(int i = 0; i < pm.size(); ++i) pm.set(i, pm.get(i) + Float.parseFloat(features[i]) * num);
				tot += num;
			}
			for(int i = 0; i < pm.size(); ++i) pm.set(i, pm.get(i) / tot);	// 计算同簇点的均值
			
			context.write(key, new Text(Helper.ArrayList2String(pm)));	// key -> 类ID, value -> 当前点的10维数据 (f1,f2,...)
		}
	}
	
	/* main class */
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			
			String initialCentersPath = args[0];	// 初始中心
			String dataSet = args[1];	// 数据集
			String tmpDir = "/cache/";	// 临时文件夹（存放中间中心点计算结果）
			String outputDir = args[2];	// 输出文件夹
			String curCenterFile = tmpDir + "curCenter";	// 本轮使用的中心
			String nxtCenterFile = tmpDir + "nxtCenter";	// 本轮算出，下轮使用的中心
			String tmpOutputDir = tmpDir + "/tmpout/";	// 临时输出
			Integer numIteration = 0;	// 当前迭代轮数
			Integer maxIteration = Integer.parseInt(args[3]);	// 最大迭代轮数
			 
			Helper.copyFile(initialCentersPath, nxtCenterFile);	// 将初始中心拷贝到临时文件夹
			
			do {
				Helper.deletePath(curCenterFile, false);
				Helper.copyFile(nxtCenterFile, curCenterFile);
				Helper.deletePath(nxtCenterFile, false);
				
				Job job = Job.getInstance(conf, "Kmeans （Round: " + numIteration.toString() + ")");	// 本轮迭代任务
				
				job.setJarByClass(Kmeans.class);	// 设置job类
				
				job.addCacheFile(new Path(curCenterFile).toUri());	// 分发当前中心点集合文件到各Worker
				
				job.setMapperClass(KmeansMapper.class);	// 设置 Mapper
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(Text.class);
				
				job.setCombinerClass(KmeansCombiner.class);	// 设置 Combiner
				
				job.setReducerClass(KmeansReducer.class);	// 设置 Reducer
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job, new Path(dataSet));	// 输入文件为训练集合
				FileOutputFormat.setOutputPath(job, new Path(tmpOutputDir));	// 输出文件为最新中心点集合
				
				job.waitForCompletion(true);	// 等待任务完成
				
				Helper.copyFile(tmpOutputDir + "part-r-00000", nxtCenterFile);
				Helper.deletePath(tmpOutputDir, true);
				numIteration++;	// 增加迭代轮数
				
			} while(!Helper.isConverged(curCenterFile, nxtCenterFile) && numIteration <= maxIteration);	// 已经收敛或超过最大迭代轮数则停止
			Helper.copyFile(nxtCenterFile , outputDir + "/output");
			Helper.deletePath(tmpDir, true);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
