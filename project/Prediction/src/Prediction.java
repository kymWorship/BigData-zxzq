import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.Vector;

public class Prediction {
    // TODO: Choose better eta!
    private static int eta1 = 16;
    private static int eta2 = 1;
    private static float[] wHome = new float[9];
    private static float[] wAway = new float[9];
    private static float[][] wConvolutionAwayHome = new float[9][9];
    private static float ratio = 1;
    private static float sigmoidConvolutionKernel(float[] propAway, float[] propHome) {
        float temp = 0;
        for (int i = 0; i < 9; i++ ) {
            temp += (wAway[i]*propAway[i] - wHome[i]*propHome[i]);
            for (int j = 0; j < 9; j++ ) {
                temp += wConvolutionAwayHome[i][j]*wAway[i]*wHome[j];
            }
        }
        return temp;
    }
    private static float sigmoidConvolution(float[] propAway, float[] propHome) {
        return (float) (1 / (1 + Math.exp(-sigmoidConvolutionKernel(propAway, propHome))));
    }
    private  static float sigmoidFinal(float kernel, float rAway, float rHome, int k, float[] rotAway, float[] rotHome) {
        float temp = kernel*ratio + rAway - rHome;
        for (int i = 0; i < k; i++) {
            temp += rotAway[2*i]*rotHome[2*i+1] - rotAway[2*i+1]*rotHome[2*i];
        }
        return (float) (1 / (1 + Math.exp(-temp)));
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration1, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        // 1. obtain Data of each Match
        // * Properties: a) 2-pt hit rate b) 3-pt hit rate c) assist
        //               d) block         e) foul          f) free throw hit rate
        //               g) turnoverPlay  h) turnoverCause i) exchange
        // * Note: choose less properties in case lack of data
        // * Output: TeamAway: Score, Pa, Pb, ..., Pi; TeamHome: Score, Pa, Pb, ..., Pi
        Job job1 = new Job(configuration1, "MatchRate");
        job1.setJarByClass(Prediction.class);
        Path in = new Path(otherArgs[0]);
        Path matchRateOutput = new Path(otherArgs[1]+"/tmp/MatchRate");
        FileInputFormat.addInputPath(job1, in);
        FileSystem fs1 = FileSystem.get(configuration1);
        if (fs1.exists(matchRateOutput)) {
            fs1.delete(matchRateOutput, true);
        }
        FileOutputFormat.setOutputPath(job1, matchRateOutput);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(MatchRateMapper.class);
        job1.setMapOutputKeyClass(Match.class);
        job1.setMapOutputValueClass(RateRecord.class);
        job1.setReducerClass(MatchRateReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        job1.waitForCompletion(true);

        // 2. Reduce to data of each Team
        // * Output: Team: Pa, Pb, ..., Pi
        Configuration configuration2 = new Configuration();
        Job job2 = new Job(configuration2, "TeamRate");
        job2.setJarByClass(Prediction.class);
        Path in2 = matchRateOutput;
        Path teamRateOutput = new Path(otherArgs[1] + "/tmp/TeamRate");
        FileInputFormat.addInputPath(job2, in2);
        FileSystem fs2 = FileSystem.get(configuration2);
        if (fs2.exists(teamRateOutput)) {
            fs2.delete(teamRateOutput, true);
        }
        FileOutputFormat.setOutputPath(job2, teamRateOutput);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(TeamRateMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(TeamRateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.waitForCompletion(true);

        // 3. Read in match results and learn with convolution
        Vector<String> matchRecordAwayTeam = new Vector<>();
        Vector<String> matchRecordHomeTeam = new Vector<>();
        Vector<Boolean> matchRecordResults = new Vector<>();
        FileStatus[] matchRateFiles = fs1.listStatus(matchRateOutput);
        for (FileStatus file: matchRateFiles) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs1.open(file.getPath())));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] tempData = line.split(";");
                    String[] awayPropsString = tempData[0].split(":")[1].split(",");
                    String[] homePropsString = tempData[1].split(":")[1].split(",");
                    matchRecordAwayTeam.add(tempData[0].split(":")[0]);
                    matchRecordHomeTeam.add(tempData[1].split(":")[0]);
                    float[] awayProps = new float[9];
                    float[] homeProps = new float[9];
                    for (int i = 0; i < 9; i++) {
                        awayProps[i] = Float.parseFloat(awayPropsString[i+1]);
                        homeProps[i] = Float.parseFloat(homePropsString[i+1]);
                    }
                    matchRecordResults.add(Integer.parseInt(awayPropsString[0]) > Integer.parseInt(homePropsString[0]));
                    float delta = (float) ( (Integer.parseInt(awayPropsString[0]) > Integer.parseInt(homePropsString[0])) ? 1 : 0 ) - sigmoidConvolution(awayProps, homeProps);
                    for (int i = 0; i < 9; i++) {
                        wAway[i] += eta1*delta*awayProps[i];
                        wHome[i] -= eta1*delta*homeProps[i];
                        for(int j = 0; j < 9; j++) {
                            wConvolutionAwayHome[i][j] += eta2*delta*awayProps[i]*homeProps[j];
                        }
                    }
                    line = br.readLine();
                }
            }finally {
                br.close();
            }
        }
        System.out.println("First Stage Finish!");

        // 4. Read in team results
        Vector<String> teamList = new Vector<>();
        Vector<float[]> teamProperties = new Vector<>();
        FileStatus[] teamRateFiles = fs2.listStatus(teamRateOutput);
        for (FileStatus file:teamRateFiles) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs2.open(file.getPath())));
            try {
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] tempData = line.split(",");
                    teamList.add(tempData[0]);
                    float[] properties = new float[9];
                    for (int i = 0; i < 9; i++) {
                        properties[i] = Float.parseFloat(tempData[i+1]);
                    }
                    teamProperties.add(properties);
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
        }

        // learn Final Prediction with mELO
        int k = 2;
        float[] rAwayList = new float[teamList.size()];
        float[] rHomeList = new float[teamList.size()];
        float[][] rotList = new float[teamList.size()][2*k];
        // init rotList (0 will become trivial)
        for (int i = 0; i < teamList.size(); i++) {
            for (int j = 0; j < 2*k; j++) {
                rotList[i][j] = 1; // TODO: choose better init value!
            }
        }
        for (int i = 0; i < matchRecordResults.size(); i++) { // Traverse match
            int awayIndex = teamList.indexOf(matchRecordAwayTeam.get(i));
            assert(awayIndex>=0);
            int homeIndex = teamList.indexOf(matchRecordHomeTeam.get(i));
            assert(homeIndex>=0);
            float kernel = sigmoidConvolutionKernel(teamProperties.get(awayIndex), teamProperties.get(homeIndex));
            float estimated = sigmoidFinal(kernel, rAwayList[awayIndex], rHomeList[homeIndex], k, rotList[awayIndex], rotList[homeIndex]);
            float delta = (float) (matchRecordResults.get(i) ? 1 : 0) - estimated;
            ratio += eta2*delta*kernel;
            rAwayList[awayIndex] += eta2*delta;
            rHomeList[homeIndex] -= eta2*delta;
            for (int j = 0; j < k; j++) {
                rotList[awayIndex][2*j] += eta2*delta*rotList[homeIndex][2*j+1];
                rotList[awayIndex][2*j+1] -= eta2*delta*rotList[homeIndex][2*j];
                rotList[homeIndex][2*j] -= eta2*delta*rotList[awayIndex][2*j+1];
                rotList[homeIndex][2*j+1] += eta2*delta*rotList[awayIndex][2*j];
            }
        }

    }
}

class MatchRateMapper extends Mapper<LongWritable, Text, Match, RateRecord> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.equals(new LongWritable(0))) {return;} // skip title
        String[] props = value.toString().split(",", 26);
        String date = props[1];
        String awayTeam = props[4];
        String homeTeam = props[5];
        String playTeam = props[6];
        String againstTeam = (playTeam.equals(awayTeam)) ? homeTeam : awayTeam;
        // context: key: team; value: {date, typo, numb}
        // - Shot a: 2-pt b: 3-pt
        if (!props[7].equals("")) {
            String shotType = props[8].substring(0, 1);
            String tmpTypo = (shotType.equals("2")) ? "a" : "b";
            String shotOutcome = props[9];
            int tmpRes = shotOutcome.equals("make") ? 1 : 0;
            int tmpScore = shotOutcome.equals("make") ? Integer.parseInt(shotType) : 0;
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(tmpScore, playTeam + "-" + tmpTypo, tmpRes));
        }
        // - Assist c
        if (!props[10].equals("")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, playTeam + "-" + "c", 1));
        }
        // - Block d
        if (!props[11].equals("")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, againstTeam + "-" + "d", 1));
        }
        // - Foul e
        if (!props[12].equals("")) {
            if (props[12].equals("offensive") || props[12].equals("loose ball")) {
                context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, playTeam + "-" + "e", 1));
            }
            else {
                context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, againstTeam + "-" + "e", 1));
            }
        }
        // - Free throw f
        if (props[20].equals("make")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(1, playTeam + "-" + "f", 1));
        } else if ( props[20].equals("miss")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, playTeam + "-" + "f", 0));
        }
        // - Turnover Play: g, Cause: h
        if (!props[24].equals("")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, playTeam + "-" + "h", 1));
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, againstTeam + "-" + "g", 1));
        }
        // - Exchange: i
        if (!props[21].equals("")) {
            context.write(new Match(date, homeTeam, awayTeam), new RateRecord(0, playTeam + "-" + "i", 1));
        }
    }
}

class MatchRateReducer extends Reducer<Match, RateRecord, Text, NullWritable> {
    @Override
    protected void reduce(Match key, Iterable<RateRecord> values, Context context) throws IOException, InterruptedException {
        int awayOccurrence[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
        int homeOccurrence[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
        int awayScore = 0;
        int homeScore = 0;
        int awayMake[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
        int homeMake[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
        float awayRates[] = new float[9];
        float homeRates[] = new float[9];
        String homeTeam = key.getHome();
        String awayTeam = key.getAway();
        for (RateRecord value: values) {
            String[] tempTypo = value.getType().split("-");
            //System.out.println("team:"+tempTypo[0]+",type:"+tempTypo[1]);
            if (homeTeam.equals(tempTypo[0])) {
                homeScore += value.getScore();
                homeMake[tempTypo[1].charAt(0) - 'a'] += value.getNumb();
                homeOccurrence[tempTypo[1].charAt(0) - 'a'] += 1;
            } else {
                awayScore += value.getScore();
                awayMake[tempTypo[1].charAt(0) - 'a'] += value.getNumb();
                awayOccurrence[tempTypo[1].charAt(0) - 'a'] += 1;
            }
        }
        for (int i = 0; i < 9; i ++ ) {
            homeRates[i] = (float)homeOccurrence[i];
            awayRates[i] = (float)awayOccurrence[i];
        }
        homeRates[0] = (homeOccurrence[0]==0) ? 0 : (float)homeMake[0] / homeOccurrence[0];
        homeRates[1] = (homeOccurrence[1]==0) ? 0 : (float)homeMake[1] / homeOccurrence[1];
        homeRates[5] = (homeOccurrence[5]==0) ? 0 : (float)homeMake[5] / homeOccurrence[5];
        awayRates[0] = (awayOccurrence[0]==0) ? 0 : (float)awayMake[0] / awayOccurrence[0];
        awayRates[1] = (awayOccurrence[1]==0) ? 0 : (float)awayMake[1] / awayOccurrence[1];
        awayRates[5] = (awayOccurrence[5]==0) ? 0 : (float)awayMake[5] / awayOccurrence[5];
        String homeOutput = homeTeam + ":" + Integer.toString(homeScore);
        String awayOutput = awayTeam + ":" + Integer.toString(awayScore);
        for (int i = 0; i < 9; i ++ ) {
            homeOutput += ("," + Float.toString(homeRates[i]));
            awayOutput += ("," + Float.toString(awayRates[i]));
        }
        context.write(new Text(awayOutput+";"+homeOutput), NullWritable.get());
        // * Output: TeamAway: Score, Pa, Pb, ..., Pi; TeamHome: Score, Pa, Pb, ..., Pi
    }
}

class TeamRateMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split(";");
        String[] awayRecord = records[0].split(":");
        String[] homeRecord = records[1].split(":");
        context.write(new Text(awayRecord[0]), new Text(awayRecord[1]));
        context.write(new Text(homeRecord[0]), new Text(homeRecord[1]));
    }
}
class TeamRateReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float[] properties = new float[9];
        int matchNumb = 0;
        for (Text value: values) {
            String[] tempData = value.toString().split(",");
            assert(tempData.length == 10);
            for ( int i = 0; i < 9; i ++ ) {
                properties[i] += Float.parseFloat(tempData[i+1]);
            }
            matchNumb ++;
        }
        String output = key.toString();
        for (int i = 0; i < 9; i++) {
            properties[i] /= matchNumb;
            output += (","+Float.toString(properties[i]));
        }
        context.write(new Text(output), NullWritable.get());
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

class RateRecord implements WritableComparable<RateRecord> {
    private int score;
    private String typo;
    private int numb;

    public RateRecord() {
        super();
    }
    public RateRecord(int score, String typo, int numb) {
        super();
        this.setScore(score);
        this.setType(typo);
        this.setNumb(numb);
    }

    @Override
    public String toString() {
        String res = Integer.toString(score)+","+typo+","+Integer.toString(numb);
        return res;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.score);
        out.writeUTF(this.typo);
        out.writeInt(this.numb);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.score = in.readInt();
        this.typo = in.readUTF();
        this.numb = in.readInt();
    }
    @Override
    public int compareTo(RateRecord o) {
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

    public void setScore(int score) {this.score = score;}
    public int getScore() {return this.score;}
    public void setType(String typo) {this.typo = typo;}
    public String getType() {return this.typo;}
    public void setNumb(int numb) {this.numb = numb;}
    public int getNumb() {return this.numb;}
}

class Match implements WritableComparable<Match> {
    private String date;
    private String home;
    private String away;
    //private int home_score;
    //private int away_score;

    public Match() {
        super();
    }
    public Match(String date, String home, String away) {
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
    public int compareTo(Match o) {
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

