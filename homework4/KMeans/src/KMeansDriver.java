

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 参考网页：
// https://blog.csdn.net/u013006675/article/details/75085682
// https://blog.csdn.net/garfielder007/article/details/51612697

public class KMeansDriver{
    public static void main(String[] args) throws Exception{
        int repeated = 0;
        System.out.println(args[1]);
        if (args.length != 5){
            System.err.println("Usage:<points_folder> <init_center_folder> <out> <k> <threshold>");
            System.exit(2);
        }

        // Test Group
        //Configuration testConf = new Configuration();
        //FileSystem hdfs = FileSystem.get(testConf);
        //Path inputDir = new Path(args[0]);
        //FileStatus[] inputFiles = hdfs.listStatus(inputDir);
        //for (FileStatus File :inputFiles) {
        //    System.out.println(File.toString());
        //}


        String tempFolder = args[2] + "/temp";
        String currentInputCenter = args[1];
        String currentOutputCenter= tempFolder + "/center_" + Integer.toString(repeated);
        Path in = new Path(args[0]);
        boolean flag = true;
        // 不断提交MapReduce作业指导相邻两次迭代聚类中心的距离小于阈值(参数自己调，设置为0.001吧，作业要求是均值不变化，所以设置了阈值）
        // 输入的四个参数：jar <points_folder> <init_center_folder> <out_folder> <k_num> <threshold>
        while (flag){
            if( repeated != 0 ) {
                currentInputCenter = currentOutputCenter;
                currentOutputCenter= tempFolder + "/center_" + Integer.toString(repeated);
            }
            Configuration conf = new Configuration();
            //conf.set("centerpath", currentInputCenter);  //Last聚类中心文件
            conf.set("kpath", args[3]);  //聚类数
            conf.set("repeated", Integer.toString(repeated));
            Job job = new Job(conf, "KMeansCluster");//新建MapReduce作业
            job.setJarByClass(KMeansDriver.class);//设置作业启动类

            FileInputFormat.addInputPath(job, in);//设置输入路径
            //FileSystem fs = FileSystem.get(conf);
            //if (fs.exists(out)){//如果输出路径存在，则先删除之
            //    fs.delete(out, true);
            //}
            FileOutputFormat.setOutputPath(job, new Path(currentOutputCenter));//设置输出路径

            job.addCacheFile(new Path(currentInputCenter).toUri());

            job.setMapperClass(KMeansMapper.class);//设置Map类
            job.setReducerClass(KMeansReducer.class);//设置Reduce类

            job.setOutputKeyClass(IntWritable.class);//设置输出键的类
            job.setOutputValueClass(Text.class);//设置输出值的类

            job.waitForCompletion(true);//启动作业
            ++repeated;
            if( repeated != 0 ) {
                // update flag (= notFinish)
                flag = !Assistance.isFinished(currentInputCenter, currentOutputCenter, Integer.parseInt(args[3]), Float.parseFloat(args[4]));
            }


        }
        //最后一次循环得到的结果根据作为最终得到的聚类中心对数据集再进行聚类
        //System.out.println("We have repeated " + repeated + " times.");
        //Cluster(args, currentOutputCenter);
    }


    //public static void Cluster(String[] args, String lastOutputPath) throws IOException, InterruptedException, ClassNotFoundException{
    //    Configuration conf = new Configuration();
    //    if (args.length != 5){
    //        System.err.println("Usage: <points_folder> <init_center_folder> <out> <k> <threshold>");
    //        System.exit(2);
    //    }
    //    conf.set("centerpath", lastOutputPath);  //最后一次聚类中心文件
    //    conf.set("kpath", args[3]);
    //    Job job = new Job(conf, "KMeansCluster");
    //    job.setJarByClass(KMeansDriver.class);
//
    //    Path in = new Path(args[0]);
    //    Path out = new Path(args[2]);
    //    FileInputFormat.addInputPath(job, in);
    //    FileSystem fs = FileSystem.get(conf);
    //    if (fs.exists(out)){
    //        fs.delete(out, true);
    //    }
    //    FileOutputFormat.setOutputPath(job, out);
    //    //因为只是将样本点聚类，不需要reduce操作，故不设置Reduce类
    //    job.setMapperClass(KMeansMapper.class);
    //    job.setOutputKeyClass(IntWritable.class);
    //    job.setOutputValueClass(Text.class);
    //    job.waitForCompletion(true);
    //}
}

class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
    private ArrayList<ArrayList<Float>> centers = new ArrayList<ArrayList<Float>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        URI[] uriList = context.getCacheFiles();
        assert(uriList.length==1);
        URI centerFileUri = uriList[0];
        String centerFileName = new Path(centerFileUri.getPath()).getName().toString();
        if ( Integer.parseInt(context.getConfiguration().get("repeated")) != 0 ) {
            centerFileName += "/part-r-00000";
        }
        BufferedReader centersReader = new BufferedReader(new FileReader(centerFileName)); // 创建文件读取器

        String line;    // 文件中的一行
        while((line = centersReader.readLine()) != null) {
            String id = line.split("\\s+")[0];
            String[] features = line.split("\\s+")[1].split(",");   // 获取该点各维数值
            ArrayList<Float> center = new ArrayList<Float>();   // 用于存储该点
            for(String feature : features) center.add(Float.parseFloat(feature));   // 类型转化
            assert(center.size() == 10);
            centers.add(center);    // 存到点集中

        }
        centersReader.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] fields = line.split(" ");
        //List<ArrayList<Float>> centers = Assistance.getCenters(context.getConfiguration().get("centerpath"));
        int k = Integer.parseInt(context.getConfiguration().get("kpath"));
        float minDist = Float.MAX_VALUE;
        int centerIndex = k;
        //计算样本点到各个中心的距离，并把样本聚类到距离最近的中心点所属的类
        for (int i = 0; i < k; ++i){
            float currentDist = 0;
            for (int j = 0; j < fields.length-1; ++j){
                float tmp = Math.abs(this.centers.get(i).get(j) - Float.parseFloat(fields[j+1]));
                currentDist += Math.pow(tmp, 2);
            }
            if (minDist > currentDist){
                minDist = currentDist;
                centerIndex = i;
            }
        }
        context.write(new IntWritable(centerIndex), new Text(value));
    }
}

class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
        List<ArrayList<Float>> assistList = new ArrayList<ArrayList<Float>>();
        String tmpResult = "";
        for (Text val : value){
            String line = val.toString();
            String[] fields = line.split(" ");
            List<Float> tmpList = new ArrayList<Float>();
            for (int i = 1; i < fields.length; ++i){
                tmpList.add(Float.parseFloat(fields[i]));
            }
            assistList.add((ArrayList<Float>) tmpList);
        }
        //计算新的聚类中心
        for (int i = 0; i < assistList.get(0).size(); ++i){
            float sum = 0;
            for (int j = 0; j < assistList.size(); ++j){
                sum += assistList.get(j).get(i);
            }
            float tmp = sum / assistList.size();
            if (i == 0){
                tmpResult += tmp;
            }
            else{
                tmpResult += "," + tmp;
            }
        }
        Text result = new Text(tmpResult);
        context.write(key, result);
    }
}
