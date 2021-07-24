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

public class Count{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: <int> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Count");//新建MapReduce作业
        job.setJarByClass(Count.class);//设置作业启动类
        Path in = new Path(otherArgs[0]);  //输入文件路径
        Path out = new Path(otherArgs[1]); //输出文件路径
        FileInputFormat.addInputPath(job, in);//设置输入路径
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)){//如果输出路径存在，则先删除之
            fs.delete(out, true);
        }
        FileOutputFormat.setOutputPath(job, out);//设置输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);

	}

}

class CountMapper extends Mapper<LongWritable, Text, Order, Text> {
	protected int cnt = 0;
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/* We get a line of record and return (date, home, away) as key and temp score of (home_score, away_score)
		 * ! Please note that we return two scores when one score event happens in order to align the output data !
		 */
		if(key.equals(new LongWritable(0))){return;} // skip title
		FileSplit fs = (FileSplit) context.getInputSplit();
		String[] props = value.toString().split("\\,",26);
		System.out.println(props.length);
		String date = props[1];
		String away = props[4];
		String home = props[5];
		String playby = props[6];
		int pri = 0;
		if(playby.equals(home)){
			pri = 1;
		}
		String shottype = props[8];
		int home_score = 0;
		int away_score = 0;
		Order od = new Order(date, home, away);
		// 2 or 3 point shot
		if(!shottype.equals("")){
			if(props[9].equals("miss")){
				;
			}
			else if(shottype.substring(0,1).equals("2")){
				home_score += 2*pri;
				away_score += 2*(1-pri);
			} else {
				home_score += 3*pri;
				away_score += 3*(1-pri);
			}
			context.write(od, new Text(Integer.toString(home_score)+","+Integer.toString(away_score)));

		}
		// 1 point shot
		if(props[20].equals("make")){
			home_score += pri;
			away_score += 1-pri;
			context.write(od, new Text(Integer.toString(home_score)+","+Integer.toString(away_score)));

		}
	}
}

class CountReducer extends Reducer<Order, Text, OrderBean, NullWritable> {
	@Override
	protected void reduce(Order key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int home_score = 0;
		int away_score = 0;
		for(Text itr: values) {
			String[] two = itr.toString().split("\\,");
			int tmphome = Integer.parseInt(two[0]);
			int tmpaway = Integer.parseInt(two[1]);
			home_score += tmphome;
			away_score += tmpaway;
		}
		OrderBean od = new OrderBean(key, home_score, away_score);
		context.write(od, NullWritable.get());

	}
}

class OrderBean implements WritableComparable<OrderBean> {
	//This class is to store date, home_team(home), away_team(away), 
	//total score of home->home_score and away->away_score
	//sorted by 1.date 2.home team name.
	private String date;
	private String home;
	private String away;
	private int home_score;
	private int away_score;

	public OrderBean() {
		super();
	}
	public OrderBean(Order od, int home_score, int away_score) {
		super();
		this.setDate(od.getDate());
		this.setHome(od.getHome());
		this.setAway(od.getAway());
		this.setHomeScore(home_score);
		this.setAwayScore(away_score);
	}
	public OrderBean(String date, String home, String away, String playby, int count) {
		super();
		this.setDate(date);
		this.setHome(home);
		this.setAway(away);
		this.setHomeScore(home_score);
		this.setAwayScore(away_score);

	}

	@Override
	public String toString() {
		String res = date+","+home+","+Integer.toString(home_score)+","+away+","+Integer.toString(away_score);
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.date);
		out.writeUTF(this.home);
		out.writeInt(this.home_score);
		out.writeUTF(this.away);
		out.writeInt(this.away_score);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.home = in.readUTF();
		this.home_score = in.readInt();
		this.away = in.readUTF();
		this.away_score = in.readInt();
	}
	@Override
	public int compareTo(OrderBean o) {
		int ans1 = date.compareTo(o.date);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			int ans2 = home.compareTo(o.home);
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

	public void setDate(String date) {this.date = date;}
	public String getDate() {return this.date;}
	public void setHome(String home) {this.home = home;}
	public String getHome() {return this.home;}
	public void setAway(String away) {this.away = away;}
	public String getAway() {return this.away;}
	public void setHomeScore(int sc) {this.home_score = sc;}
	public void setAwayScore(int sc) {this.away_score = sc;}

}



class Order implements WritableComparable<Order> {
	private String date;
	private String home;
	private String away;
	//private int home_score;
	//private int away_score;

	public Order() {
		super();
	}
	public Order(String date, String home, String away) {
		super();
		this.setDate(date);
		this.setHome(home);
		this.setAway(away);
		//this.setHomeScore(home_score);
		//this.setAwayScore(away_score);

	}

	@Override
	public String toString() {
		//String res = date+","+home+","+home_score.toString()+","+away+","+away_score.toString();
		String res = date+","+home+","+away;
		return res;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.date);
		out.writeUTF(this.home);
		//out.writeInt(this.home_score);
		out.writeUTF(this.away);
		//out.writeInt(this.away_score);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.date = in.readUTF();
		this.home = in.readUTF();
		//this.home_score = in.readInt();
		this.away = in.readUTF();
		//this.away_score = in.readInt();
	}
	@Override
	public int compareTo(Order o) {
		int ans1 = date.compareTo(o.date);
		ans1 = clampp(ans1);
		if(ans1 == 0){
			int ans2 = home.compareTo(o.home);
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

	public void setDate(String date) {this.date = date;}
	public String getDate() {return this.date;}
	public void setHome(String home) {this.home = home;}
	public String getHome() {return this.home;}
	public void setAway(String away) {this.away = away;}
	public String getAway() {return this.away;}


	//public void setHomeScore(int sc) {this.home_score = sc;}
	//public void setAwayScore(int sc) {this.away_score = sc;}
}

