

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

import java.io.IOException;
import java.util.*;

public class Assistance {

    //读取聚类中心点信息：聚类中心ID、聚类中心点
    public static List<ArrayList<Float>> getCenters(String inputPath){// input:Folder
        List<ArrayList<Float>> result = new ArrayList<ArrayList<Float>>();
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path inputDir = new Path(inputPath);
            FileStatus[] inputFiles = hdfs.listStatus(inputDir);
            for (FileStatus in:inputFiles) {
                FSDataInputStream fsIn = hdfs.open(in.getPath());
                LineReader lineIn = new LineReader(fsIn, conf);
                Text line = new Text();
                while (lineIn.readLine(line) > 0) {
                    String record = line.toString();
                    //Hadoop输出键值对时会在键跟值之间添加制表符，所以用空格代替之。
                    //PS：这里得到聚类中心的函数，老师给的聚类中心是以逗号隔开的，但我们之后自己的是空格（？），可能要调整一下。
                    String[] fields = record.replace("\t", " ").split(" ");
                    String[] fields_new = fields[1].split(",");
                    List<Float> tmplist = new ArrayList<Float>();
                    for (int i = 0; i < fields_new.length; ++i) {
                        tmplist.add(Float.parseFloat(fields_new[i]));
                    }
                    result.add((ArrayList<Float>) tmplist);
                }
                fsIn.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return result;
    }





    //计算相邻两次迭代结果的聚类中心的距离，判断是否满足终止条件
    public static boolean isFinished(String oldpath, String newpath, int k, float threshold) throws IOException{
        List<ArrayList<Float>> oldcenters = Assistance.getCenters(oldpath);
        List<ArrayList<Float>> newcenters = Assistance.getCenters(newpath);
        float distance = 0;
        for (int i = 0; i < k; ++i){
            //System.out.println("Rows:"    + oldcenters.size());
            //System.out.println("Columns:" + oldcenters.get(i).size());
            //System.out.println("Rows:"    + newcenters.size());
            //System.out.println("Columns:" + newcenters.get(i).size());
            for (int j = 0; j < oldcenters.get(i).size(); ++j){
                float tmp = Math.abs(oldcenters.get(i).get(j) - newcenters.get(i).get(j));
                distance += Math.pow(tmp, 2);
            }
        }
        //System.out.println("Distance = " + distance + " Threshold = " + threshold);
        if (distance < threshold)
            return true;
        return false;
    }
}
