import java.io.DataInput;
import java.io.DataOutput;
import java.io.*;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dist{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: <int> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Dist");//新建MapReduce作业
        job.setJarByClass(Dist.class);//设置作业启动类
        Path in = new Path(otherArgs[0]);  //输入文件路径
        Path out = new Path(otherArgs[1]); //输出文件路径
        FileInputFormat.addInputPath(job, in);//设置输入路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)){//如果输出路径存在，则先删除之
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);//设置输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(DistMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);

	}

}

class DistMapper extends Mapper<Object, Text, Text, NullWritable> {
    int cnt = 0;
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    if(cnt==0){cnt=1;return;}
		FileSplit fs = (FileSplit) context.getInputSplit();
		String[] props = value.toString().split(",",26);
		//int idx = Integer.parseInt(props[0]);
		if (props[20].equals("make")){
			context.write(new Text(props[0]+","+props[6]+" 得 1 分"), NullWritable.get());
		} else if (props[9].equals("make")) {
			context.write(new Text(props[0]+","+props[6]+" 得 "+props[8].substring(0,1)+" 分"), NullWritable.get());
		}else {
			context.write(new Text(props[0]+ ",不得分"),NullWritable.get());
		}
	}
}