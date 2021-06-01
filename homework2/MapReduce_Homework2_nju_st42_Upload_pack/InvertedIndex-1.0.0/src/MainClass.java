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

public class MainClass {
    public MainClass() {}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println(otherArgs[0]);
            System.err.println("Usage: MainClass <in> <out>");
            System.exit(2);
        }

        // Setting jobInvertedIndex
        Job jobInvertedIndex = Job.getInstance(conf, "Invert Index");
        jobInvertedIndex.setJarByClass(MainClass.class);
        jobInvertedIndex.setMapperClass(MainClass.InvertedIndexMapper.class);
        jobInvertedIndex.setReducerClass(MainClass.InvertedIndexReducer.class);
        jobInvertedIndex.setOutputKeyClass(Text.class);
        jobInvertedIndex.setOutputValueClass(Text.class);
        // Setting input & output dictionary for jobInvertedIndex
        FileInputFormat.addInputPath(jobInvertedIndex, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(jobInvertedIndex, new Path(otherArgs[2]+"/InvertedIndex"));
        // Execute jobInvertedIndex
        jobInvertedIndex.waitForCompletion(true);

        // Setting jobConvertFreqToKey
        Configuration convertFreqToKeyConf = new Configuration();
        convertFreqToKeyConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job jobConvertFreqToKey = Job.getInstance(convertFreqToKeyConf, "Convert Freq to Key");
        jobConvertFreqToKey.setInputFormatClass(KeyValueTextInputFormat.class);
        jobConvertFreqToKey.setJarByClass(MainClass.class);
        jobConvertFreqToKey.setMapperClass(MainClass.ConvertFreqToKeyMapper.class);
        jobConvertFreqToKey.setReducerClass(MainClass.ConvertFreqToKeyReducer.class);
        // Setting type for jobConvertFreqToKey
        jobConvertFreqToKey.setMapOutputKeyClass(Text.class);
        jobConvertFreqToKey.setMapOutputValueClass(Text.class);
        jobConvertFreqToKey.setOutputKeyClass(Text.class);
        jobConvertFreqToKey.setOutputValueClass(Text.class);
        // Setting path for jobConvertFreqToKey
        FileInputFormat.addInputPath(jobConvertFreqToKey, new Path(otherArgs[2]+"/InvertedIndex"));
        FileOutputFormat.setOutputPath(jobConvertFreqToKey, new Path(otherArgs[2]+"/temp/ConvertFreqToKey"));
        // Execute jobConvertFreqToKey
        jobConvertFreqToKey.waitForCompletion(true);

        // Setting jobSorted
        Configuration sortedConf = new Configuration();
        sortedConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job jobSorted = Job.getInstance(sortedConf, "Sorted");
        jobSorted.setInputFormatClass(KeyValueTextInputFormat.class);
        jobSorted.setNumReduceTasks(4);
        // setting types for jobSorted
        jobSorted.setMapOutputKeyClass(Text.class);
        jobSorted.setMapOutputValueClass(Text.class);
        jobSorted.setOutputKeyClass(Text.class);
        jobSorted.setOutputValueClass(Text.class);
        // Setting path for jobSorted
        FileInputFormat.addInputPath(jobSorted, new Path(otherArgs[2]+"/temp/ConvertFreqToKey"));
        FileOutputFormat.setOutputPath(jobSorted, new Path(otherArgs[2]+"/Sorted"));
        // Setting Class
        jobSorted.setJarByClass(MainClass.class);
        jobSorted.setMapperClass(MainClass.SortedMapper.class);
        jobSorted.setReducerClass(MainClass.SortedReducer.class);
        // Setting partitioner & comparator
        jobSorted.setSortComparatorClass(SortedKeyComparator.class);
        TotalOrderPartitioner.setPartitionFile(jobSorted.getConfiguration(), new Path(otherArgs[2]+"/temp/partition"));
        InputSampler.Sampler<Text, Text> sortedSampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
        InputSampler.writePartitionFile(jobSorted, sortedSampler);
        jobSorted.setPartitionerClass(TotalOrderPartitioner.class);
        String partitionFile = TotalOrderPartitioner.getPartitionFile(jobSorted.getConfiguration());
        URI uri = new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);
        jobSorted.addCacheFile(uri);
        // Execute jobSorted
        System.exit(jobSorted.waitForCompletion(true) ? 0 : 1);
    }

    // InvertedIndex mapper & reducer
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final LongWritable one = new LongWritable(1);
        private Text word = new Text();

        public InvertedIndexMapper() {}

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // default: key - lineoffset, value - line
            StringTokenizer itr = new StringTokenizer(value.toString());
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            Text fileName = new Text(fileSplit.getPath().getName());
            while (itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, fileName);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Vector<String> fileList = new Vector<String>();
        private Vector<Integer> occurrences = new Vector<Integer>();
        private String resultString = "";
        private Text resultText = new Text();

        public InvertedIndexReducer() {}

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key: word, values: filenames
            String currentFileName;
            for(Iterator itr = values.iterator(); itr.hasNext();) {
                currentFileName = itr.next().toString();
                if (!fileList.contains(currentFileName)) {  // if new file, add to list
                    fileList.add(currentFileName);
                    occurrences.add(0);
                }
                // update occurrence num
                int currentIndex = fileList.indexOf(currentFileName);
                occurrences.set(currentIndex, occurrences.get(currentIndex)+1);
            }

            // output results
            int sumOccurrence = 0;
            int fileNumber = fileList.size();
            String appendValue = "";
            for (int i = 0; i < fileNumber; i++) {
                sumOccurrence += occurrences.get(i);
                appendValue += (fileList.get(i) + ":" + occurrences.get(i));
                if (i!=(fileNumber-1)) {
                    appendValue += "; ";
                }
            }
            float averageOccurrence = (float)sumOccurrence / (float)fileNumber;
            this.resultString = String.format("%.2f", averageOccurrence) + ", " + appendValue;
            this.resultText.set(resultString);
            context.write(key, this.resultText);
        }
    }

    // ConvertFreqToKey mapper & reducer
    public static class ConvertFreqToKeyMapper extends Mapper<Text, Text, Text, Text> {

        public ConvertFreqToKeyMapper() {}

        public void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key - key, value - value (token by '\t')
            StringTokenizer inputValueTokenizer = new StringTokenizer(value.toString());
            String outputKeyString = inputValueTokenizer.nextToken(",");    // token freq
            context.write(new Text(outputKeyString), key);  // output: freq\t word
        }
    }

    public static class ConvertFreqToKeyReducer extends Reducer<Text, Text, Text, Text> {

        public ConvertFreqToKeyReducer() {}

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key: word, values: filenames
            for (Text value: values) {  // do nothing
                context.write(key, value);
            }
        }
    }

    // Sorted mapper & reducer & comparator
    public static class SortedMapper extends Mapper<Text, Text, Text, Text> {

        public SortedMapper() {}

        public void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key - key, value - value
            // do nothing
            context.write(key, value);
        }
    }

    public static class SortedReducer extends Reducer<Text, Text, Text, Text> {


        public SortedReducer() {}

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // key: word, values: filenames
            for (Text value: values) {  // switch key and values
                context.write(value, key);
            }
        }
    }

    public static class SortedKeyComparator extends WritableComparator {
        protected SortedKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable writableComparable1, WritableComparable writableComparable2) {
            float float1 = Float.parseFloat(writableComparable1.toString());
            float float2 = Float.parseFloat(writableComparable2.toString());

            return -Float.compare(float1, float2);
        }
    }

}
