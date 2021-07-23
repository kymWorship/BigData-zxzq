import com.ibm.jvm.j9.dump.extract.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

public class Pred{
	// We base it on the pre-processed log of task 2.
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: <int> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Pred");//新建MapReduce作业
        job.setJarByClass(Pred.class);//设置作业启动类
        Path in = new Path(otherArgs[0]);  //输入文件路径
        Path out = new Path(otherArgs[1]); //输出文件路径
        FileInputFormat.addInputPath(job, in);//设置输入路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)){//如果输出路径存在，则先删除之
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);//设置输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PredMapper.class);
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(PredReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);

	}

}

class PredMapper extends Mapper<Object, Text, Order, Text> {
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterrruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		String[] props = value.toString().split("\\,");
		String away = props[3];
		int away_score = Integer.parseInt(props[4]);
		String home = props[1];
		int home_score = Integer.parseInt(props[2]);
		int sig = 0;
		if(away_score > home_score) {sig = 1;}
		Order od = Order(away, home);
		context.write(od, sig.toString()+","+(1-sig).toString());
	}
}

class PredReducer extends Reducer<Order, Text, OrderBean, NullWritable> {
	@Override
	protected void reduce(Order key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int home_score = 0;
		int away_score = 0;
		for(Iterator itr = values.iterator(); itr.hasNext();) {
			String[] two = itr.next.toString().split("\\,");
			int tmphome = Integer.parseInt(two[0]);
			int tmpaway = Integer.parseInt(two[1]);
			home_score += tmphome;
			away_score += tmpaway;
		}
		OrderBean od = OrderBean(key, home_score, away_score);
		context.write(od, NullWritable.get());

	}
}

class Order implements WritableComparable<Order> {
	private String away;
	private String home;
	//private int away_score;
	//private int home_score;
	
	public Order() {
		super();
	}
	public Order(String away, String home) {
		super();
		this.setAway(away);
		this.setHome(home);
		//this.setAwayScore(away_score);
		//this.setHomeScore(home_score);

	}

	@Override
	public String toString() {
		//String res = date+","+home+","+home_score.toString()+","+away+","+away_score.toString();
		String res = away+","+home;
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.home);
		out.writeUTF(this.away);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.home = in.readUTF();
		this.away = in.readUTF();
	}
	@Override
	public int compareTo(Order o) {
		int ans1 = away.equals(o.away);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			int ans2 = home.equals(o.home);
			ans2 = clampp(ans2);
			return ans2;
		}
		return ans1;
	}
	public clampp(int x) {
		if(x > 0) {
			return 1;
		} else if(x < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	public void setHome(String home) {this.home = home;}
	public String getHome() {return this.home;}
	public void setAway(String away) {this.away = away;}
	public String getAway() {return this.away;}

}

class OrderBean implements WritableComparable<OrderBean> {
	private String away;
	private String home;
	private float away_score;
	private float home_score;
	
	public OrderBean() {
		super();
	}
	public OrderBean(String away, String home, int away_cnt, int home_cnt) {
		super();
		this.setAway(away);
		this.setHome(home);
		this.setAwayScore(away_cnt/(away_cnt+home_cnt));
		this.setHomeScore(home_cnt/(away_cnt+home_cnt));

	}
	public OrderBean(Order od, int away_cnt, int home_cnt) {
		super();
		this.setAway(od.getAway());
		this.setHome(od.getHome());
		this.setAwayScore(away_cnt/(away_cnt+home_cnt));
		this.setHomeScore(home_cnt/(away_cnt+home_cnt));

	}

	@Override
	public String toString() {
		String p1 = String.format("%.4f", away_score);
		String p2 = String.format("%.4f", home_score);
		String res = away+","+p1+","+home+","+p2;
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.away);
		out.writeFloat(this.away_score);
		out.writeUTF(this.home);
		out.writeFloat(this.home_score);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.away = in.readUTF();
		this.away_score = in.readFloat();
		this.home = in.readUTF();
		this.home_score = in.readFloat();
	}
	@Override
	public int compareTo(Order o) {
		int ans1 = away.equals(o.away);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			int ans2 = home.equals(o.home);
			ans2 = clampp(ans2);
			return ans2;
		}
		return ans1;
	}
	public clampp(int x) {
		if(x > 0) {
			return 1;
		} else if(x < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	public void setHome(String home) {this.home = home;}
	public void setAway(String away) {this.away = away;}
	public void setHomeScore(String home_score) {this.home_score = home_score;}
	public void setAwayScore(String away_score) {this.away_score = away_score;}

}