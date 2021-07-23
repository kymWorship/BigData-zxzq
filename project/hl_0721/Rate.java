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

public class Rate{
	public static void main(String[] args) throws Exception {
		// Job 1: get data of each player on each task
		// using Ratemapper, RateReducer
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: <int> <out>");
            System.exit(2);
        }
        Job job1 = new Job(conf1, "Rate");//新建MapReduce作业
        job1.setJarByClass(Rate.class);//设置作业启动类
        Path in = new Path(otherArgs[0]);  //输入文件路径
        Path out1 = new Path(otherArgs[1]+"/tmp"); //输出文件路径
        FileInputFormat.addInputPath(job1, in);//设置输入路径
        FileSystem fs1 = FileSystem.get(conf1);
        if (fs1.exists(out1)){//如果输出路径存在，则先删除之
            fs1.delete(out1, true);
        }
        FileOutputFormat.setOutputPath(job1, out1);//设置输出路径
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(RateMapper.class);
        job1.setMapOutputKeyClass(Order.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(RateReducer.class);
        job1.setOutputKeyClass(OrderBean.class);
        job1.setOutputValueClass(NullWritable.class);
        job1.waitForCompletion(true);


        // Job 2: rank players on each tasks and output top five
        // using RankMapper and RankReducer
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Rank");//新建MapReduce作业
        job2.setJarByClass(Rate.class);//设置作业启动类
        Path in2 = out1;  //输入文件路径
        Path out2 = new Path(otherArgs[1]+"/res"); //输出文件路径
        FileInputFormat.addInputPath(job2, in2);//设置输入路径
        FileSystem fs2 = FileSystem.get(conf2);
        if (fs2.exists(out2)){//如果输出路径存在，则先删除之
            fs2.delete(out2, true);
        }
        FileOutputFormat.setOutputPath(job2, out2);//设置输出路径
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(RankMapper.class);
        job2.setMapOutputKeyClass(OrderBean.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setNumReduceTasks(1);
        job2.setReducerClass(RankReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.waitForCompletion(true);

	}

}

class RankMapper extends Mapper<Object, Text, OrderBean, NullWritable> {
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//read data to a key(OrderBean) for system sort
		FileSplit fs = (FileSplit) context.getInputSplit();
		String[] props = value.toString().split("\\,");
		OrderBean od = new OrderBean(props[0], props[1], Integer.parseInt(props[2]));
		context.write(od, NullWritable.get());
	}
}

class RateMapper extends Mapper<Object, Text, Order, IntWritable> {
	protected int cnt = 0;
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if(cnt==0){cnt=1;return;}
		FileSplit fs = (FileSplit) context.getInputSplit();
		String[] props = value.toString().split("\\,",26);
		//============== Score ===================
		String score_player = props[7];
		if (props[20].equals("make")){
			Order od1 = new Order(score_player, "得分");
			context.write(od1, new IntWritable(1));
		} else if (props[9].equals("make")) {
			Order od1 = new Order(score_player, "得分");
			context.write(od1, new IntWritable(Integer.parseInt(props[8].substring(0,1))));
		}
		//============== Rebound =================
		String rbd_player = props[14];
		if (!rbd_player.equals("")) {
			Order od2 = new Order(rbd_player, "篮板");
			context.write(od2, new IntWritable(1));
		}
		//============== Assist ==================
		String assister = props[10];
		if (!assister.equals("")) {
			Order od3 = new Order(assister, "助攻");
			context.write(od3, new IntWritable(1));
		}
		//============== TurnOver ================
		String causer = props[25];
		if (!causer.equals("")) {
			Order od4 = new Order(causer, "抢断");
			context.write(od4, new IntWritable(1));
		}
		//============== Block ===================
		String blocker = props[11];
		if (!blocker.equals("")) {
			Order od5 = new Order(blocker, "盖帽");
			context.write(od5, new IntWritable(1));
		}
	}
}

class RankReducer extends Reducer<OrderBean, NullWritable, Text, NullWritable> {
	// We get data of all players on all tasks. Already sorted by 1.task name 2.scores
	// The idea is to iterate through the first five ones of each task.
	protected String task = "";
	protected int cnt = 0;
	protected int rnk = 1;
	protected int m_score = 0;
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
		if(task.equals("")){
			task = key.getType();
			m_score = key.getNumb();
		}
		if(cnt < 5) {// Comment: what if max player < 5(impossible here)
			cnt += 1;
			int tmp_score = key.getNumb();
			if(tmp_score<m_score){
				m_score = tmp_score;
				rnk += 1;
			}
			String msg = task + "," + Integer.toString(rnk) + "," + key.getName() +"," + Integer.toString(tmp_score);
			context.write(new Text(msg), NullWritable.get());
		} else if(!task.equals(key.getType())){
			cnt = 1;
			rnk = 1;
			m_score = key.getNumb();
			task = key.getType();
			String msg = task + "," + Integer.toString(rnk) + "," + key.getName() + "," + Integer.toString(m_score);
			context.write(new Text(msg), NullWritable.get());
		}
	}
}

class RateReducer extends Reducer<Order, IntWritable, OrderBean, NullWritable> {
	@Override
	protected void reduce(Order key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int score = 0;
		for(IntWritable itr: values) {
			int tmp = itr.get();
			score += tmp;
		}
		OrderBean od = new OrderBean(key, score);
		context.write(od, NullWritable.get());
	}
}

class Order implements WritableComparable<Order> {
	private String name;
	private String typo;
	
	public Order() {
		super();
	}
	public Order(String name, String typo) {
		super();
		this.setName(name);
		this.setType(typo);
	}

	@Override
	public String toString() {
		String res = name+","+typo;
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeUTF(this.typo);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.typo = in.readUTF();
	}
	@Override
	public int compareTo(Order o) {
		int ans1 = typo.compareTo(o.typo);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			int ans2 = name.compareTo(o.name);
			ans2 = clampp(ans2);
			return ans2;
		}
		return ans1;
	}
	public int clampp(int x) {
		if(x > 0) {
			return 1;
		} else if(x < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	public void setName(String name) {this.name = name;}
	public String getName() {return this.name;}
	public void setType(String typo) {this.typo = typo;}
	public String getType() {return this.typo;}

}

class OrderBean implements WritableComparable<OrderBean> {
	private String name;
	private String typo;
	private int numb;
	
	public OrderBean() {
		super();
	}
	public OrderBean(String name, String typo, int numb) {
		super();
		this.setName(name);
		this.setType(typo);
		this.setNumb(numb);
	}
	public OrderBean(Order od, int numb) {
		super();
		this.setName(od.getName());
		this.setType(od.getType());
		this.setNumb(numb);
	}

	@Override
	public String toString() {
		String res = name+","+typo+","+Integer.toString(numb);
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeUTF(this.typo);
		out.writeInt(this.numb);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.typo = in.readUTF();
		this.numb = in.readInt();
	}
	@Override
	public int compareTo(OrderBean o) {
		int ans1 = typo.compareTo(o.typo);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			return clampp(o.getNumb()-numb);
		}
		return ans1;
	}
	public int clampp(int x) {
		if(x > 0) {
			return 1;
		} else if(x < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	public void setName(String name) {this.name = name;}
	public String getName() {return this.name;}
	public void setType(String typo) {this.typo = typo;}
	public String getType() {return this.typo;}
	public void setNumb(int numb) {this.numb = numb;}
	public int getNumb() {return this.numb;}

}

