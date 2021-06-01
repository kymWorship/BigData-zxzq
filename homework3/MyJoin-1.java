import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

public class MyJoin {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(args[0]);
        Path tempPath = new Path(args[1]+"/temp");
        Path outputPath = new Path(args[1]+"/res");



        Configuration conf2 = new Configuration();
        FileSystem hdfs2 = FileSystem.get(conf2);
        if(hdfs2.exists(outputPath)){
            hdfs2.delete(outputPath, true);
        }

        Configuration conf1 = new Configuration();
        FileSystem hdfs1 = FileSystem.get(conf1);
        if(hdfs1.exists(tempPath)) {
            hdfs1.delete(tempPath, true);
        }

        //====================Map-Reduce task 1===========================
        Job job = Job.getInstance(conf1, "MyJoin");
        job.setJarByClass(MyJoin.class);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(JoinPartitioner.class);

        job.setReducerClass(JoinReducer.class);
        job.setNumReduceTasks(4);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, tempPath);
        job.waitForCompletion(true);

        //=====================Map-Reduce Task 2=========================

        Job job2 = Job.getInstance(conf2, "Sort");
        job2.setJarByClass(MyJoin.class);
        job2.setInputFormatClass(TextInputFormat.class);

        job2.setMapperClass(SortMapper.class);
        job2.setMapOutputKeyClass(Order.class);
        job2.setMapOutputValueClass(NullWritable.class);

        job2.setPartitionerClass(SortPartitioner.class);

        job2.setReducerClass(SortReducer.class);
        job2.setNumReduceTasks(4);
        job2.setOutputKeyClass(Order.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, outputPath);


        System.exit(job2.waitForCompletion(true)?0:1);
    }
}

// File all records from two files to keys
// This is for task "Join"
class JoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        String fileName = fs.getPath().getName();
        String[] props = value.toString().split("\\|");
        OrderBean odb = new OrderBean(props, fileName.equals("nation.tbl"));
        context.write(odb, NullWritable.get());
    }
}

// Read all the unsorted records to keys
// This is for task "Sort"
class SortMapper extends Mapper<LongWritable, Text, Order, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] props = value.toString().split("\\|");
        Order od = new Order(props);
        context.write(od, NullWritable.get());
    }
}

// Partition by nation key
// This is for task "Join"
class JoinPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {
        return key.getNationKey() % numReduceTasks;
    }
}

// Partition by supplier key
// This is for task "Sort"
class SortPartitioner extends Partitioner<Order, NullWritable> {
    @Override
    public int getPartition(Order key, NullWritable value, int numReduceTasks) {
        return key.getSuppKey() % numReduceTasks;
    }
}

// Reduce with nation key. Records from nation.tbl will come first for each nation key
// This is for task "Join"
class JoinReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    int nationKey = -1;
    String nationName = "";

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getNationKey() != nationKey) {
            nationKey = key.getNationKey();
            nationName = key.getNationName();
        } else {
            key.setNationName(nationName);
            context.write(key, NullWritable.get());
        }
    }
}

// All tasks are done, reducer just record everything
// This is for task "Sort"
class SortReducer extends Reducer<Order, NullWritable, Order, NullWritable> {
    int nationKey = -1;
    String nationName = "";

    @Override
    protected void reduce(Order key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

// This is implemented for task "Sort"
class Order implements WritableComparable<Order> {
    private int suppKey;
    private String suppName = "";
    private String suppPhone = "";
    private String nationName = "";

    public Order(){
        super();
    }
    public Order(String[] props){
        super();
        this.setSuppKey(props[0]);
        this.setSuppName(props[1]);
        this.setSuppPhone(props[2]);
        this.setNationName(props[3]);
    }

    @Override
    public String toString() {
        String res = suppKey+"|"+suppName+"|"+suppPhone+"|"+nationName;
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.nationName);
        out.writeInt(this.suppKey);
        out.writeUTF(this.suppName);
        out.writeUTF(this.suppPhone);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nationName = in.readUTF();
        this.suppKey = in.readInt();
        this.suppName = in.readUTF();
        this.suppPhone = in.readUTF();
    }

    // Since nation info is joined in, we sort by supplier key
    @Override
    public int compareTo(Order o) {
        int suppId1 = suppKey;
        int suppId2 = o.getSuppKey();
        if (suppId1 < suppId2) {
            return -1;
        } else {
            return 1;
        }
    }

    public void setNationName(String st) {this.nationName = st;}
    public String getNationName() {return this.nationName;}
    public void setSuppKey(String st) {this.suppKey = Integer.parseInt(st);}
    public int getSuppKey() {return this.suppKey;}
    public void setSuppName(String st) {this.suppName = st;}
    public String getSuppName() {return this.suppName;}
    public void setSuppPhone(String st) {this.suppPhone = st;}
    public String getSuppPhone() {return this.suppPhone;}

}

// This is implemented for task "Join"
class OrderBean implements WritableComparable<OrderBean> {
    private int suppKey;
    private String suppName = "";
    private String suppPhone = "";
    private int nationKey;
    private String nationName = "";

    public OrderBean(){
        super();
    }
    public OrderBean(String[] props, boolean isNation) {
        super();
        if(isNation){
            this.setNationKey(props[0]);
            this.setNationName(props[1]);
        } else {
            this.setSuppKey(props[0]);
            this.setSuppName(props[1]);
            this.setNationKey(props[3]);
            this.setSuppPhone(props[4]);
        }
    }

    @Override
    public String toString() {
        String res = suppKey+"|"+suppName+"|"+suppPhone+"|"+nationName;
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.nationKey);
        out.writeUTF(this.nationName);
        out.writeInt(this.suppKey);
        out.writeUTF(this.suppName);
        out.writeUTF(this.suppPhone);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nationKey = in.readInt();
        this.nationName = in.readUTF();
        this.suppKey = in.readInt();
        this.suppName = in.readUTF();
        this.suppPhone = in.readUTF();
    }

    // We priorily sort by nation key, then supplier key
    @Override
    public int compareTo(OrderBean o) {
        int nationId1 = nationKey;
        int nationId2 = o.getNationKey();
        if (nationId1 < nationId2) {
            return -1;
        } else if (nationId1 > nationId2) {
            return  1;
        }

        if (!nationName.equals("")) {
            return -1;
        } else if (!o.getNationName().equals("")) {
            return 1;
        }
        int suppId1 = suppKey;
        int suppId2 = o.getSuppKey();
        if (suppId1 < suppId2) {
            return -1;
        } else if (suppId1 > suppId2) {
            return 1;
        }

        return 0;
    }

    public void setNationKey(String st) {this.nationKey = Integer.parseInt(st);}
    public int getNationKey() {return this.nationKey;}
    public void setNationName(String st) {this.nationName = st;}
    public String getNationName() {return this.nationName;}
    public void setSuppKey(String st) {this.suppKey = Integer.parseInt(st);}
    public int getSuppKey() {return this.suppKey;}
    public void setSuppName(String st) {this.suppName = st;}
    public String getSuppName() {return this.suppName;}
    public void setSuppPhone(String st) {this.suppPhone = st;}
    public String getSuppPhone() {return this.suppPhone;}

}
