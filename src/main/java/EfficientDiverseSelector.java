import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EfficientDiverseSelector {
    @Parameter(names={"-s", "--size"}, description = "Number of users to select")
    int selectionSize = 1;
    @Parameter(names={"-i", "--input"}, description = "Path of the input file")
    String inputPath = "input";
    @Parameter(names={"-m", "--mappers"}, description = "Number of mappers")
    int numberMappers = 3;
    @Parameter(names={"-r", "--reducers"}, description = "Proportion of reducers to mappers")
    double reducersProportion = 1.75;
    private int numberReducers;

    private final static IntWritable key = new IntWritable(1);

    public static class TextArrayWritable extends ArrayWritable {

        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            super.set(texts);
        }

        @Override
        public String toString() {
            Writable[] data = super.get();
            if (data.length == 0) {
                return "";
            }

            StringBuilder sb = new StringBuilder();
            for (Writable w : data) {
                sb.append(w.toString()).append(" ");
            }
            sb.setLength(sb.length() - 1);
            return sb.toString();
        }

        protected TextArrayWritable clone() {
            TextArrayWritable textArrayWritable = new TextArrayWritable();
            textArrayWritable.set(super.get().clone());
            return textArrayWritable;
        }
    }

    private void setNumberReducers() {
        this.numberReducers = (int) Math.round(this.reducersProportion > 0 ?
                this.numberMappers * this.reducersProportion :
                this.numberMappers * 1.75);
    }

    public static EfficientUser getUser(Text userText) {
        String[] userInformation = userText.toString().split("\\s+", 3);
        String currentUser = userInformation[0];
        int currentScore = Integer.parseInt(userInformation[1]);
        String[] groups = userInformation[2].split("\\s+");
        return new EfficientUser(currentUser, currentScore, groups);
    }

    public static class GroupsRetriever
            extends Mapper<Object, Text, Text, Text> {

        private int getGroupCov() {
            return 1;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rawGroup = value.toString().split(",\\s+");
            String groupIdx = rawGroup[0];
            int groupSize = rawGroup.length -1, groupCov = getGroupCov();
            String groupInfo = groupIdx + "," + groupSize + "," + groupCov;
            for (int i = 1; i < rawGroup.length; i++) {
                context.write(new Text(rawGroup[i]), new Text(groupInfo));
            }
        }
    }

    public static class ReduceToUser
            extends Reducer<Text, Text, IntWritable, EfficientUser> {

        public void reduce(Text user, Iterable<Text> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> groupsAsList = new ArrayList<String>();

            for (Text group : groups) {
                groupsAsList.add(group.toString());
            }

            String[] groupsAsArray = new String[groupsAsList.size()];
            context.write(EfficientDiverseSelector.key, new EfficientUser(user.toString(), groupsAsList.toArray(groupsAsArray)));
        }
    }

    public static class ScoreCalculator
            extends Mapper<Object, Text, IntWritable, Text> {
        private EfficientUser lastMaxUser;
        private List<String> lastMaxUserGroups;

        protected void setup(Context context) throws IOException {
            this.lastMaxUser = readUserFromHDFS(context);
            if (lastMaxUser != null) {
                this.lastMaxUserGroups = new ArrayList<String>();
                for (Text group : this.lastMaxUser.get()) {
                    this.lastMaxUserGroups.add(group.toString().split("\\s+")[0]);
                }
            } else {
                this.lastMaxUserGroups = null;
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueAsString = value.toString();
            String userInformation = valueAsString.substring(valueAsString.indexOf('\t') + 1);
            value = new Text(userInformation);
            if (this.lastMaxUser != null) {
                EfficientUser user = EfficientDiverseSelector.getUser(value);
                List<String> newGroupsList = new ArrayList<String>();
                if (!this.lastMaxUser.getId().equals(user.getId())) {
                    for (Text group : user.get()) {
                        String[] groupInformation = group.toString().split(",");
                        String groupIdx = groupInformation[0];
                        int groupSize = Integer.parseInt(groupInformation[1]);
                        int groupCov = Integer.parseInt(groupInformation[2]);
                        if (this.lastMaxUserGroups.contains(groupIdx)) {
                            groupCov -= 1;
                        }
                        if (groupCov > 0) {
                            String groupInfo = groupIdx + "," + groupSize + "," + groupCov;
                            newGroupsList.add(groupInfo);
                        }
                    }
                    String[] newGroupsArray = new String[newGroupsList.size()];
                    context.write(EfficientDiverseSelector.key,
                            new EfficientUser(user.getId(), newGroupsList.toArray(newGroupsArray)).toText());
                }
            } else {
                context.write(EfficientDiverseSelector.key, new Text(userInformation));
            }
        }

        private EfficientUser readUserFromHDFS(Context context) throws IOException {
            FSDataInputStream os;
            BufferedReader br;
            Configuration configuration = context.getConfiguration();
            configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
            FileSystem hdfs = FileSystem.get(configuration);

            String maxUser;
            Path maxFile = new Path("max.txt");
            if (hdfs.exists(maxFile)) {
                os = hdfs.open(maxFile);
                br = new BufferedReader(new InputStreamReader(os, "UTF-8"));
                maxUser = br.readLine();
                br.close();
                hdfs.close();
                return EfficientDiverseSelector.getUser(new Text(maxUser));
            } else {
                return null;
            }
        }
    }

    public static class LocalMaxRetriever
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private static final IntWritable maxInt = new IntWritable(Integer.MAX_VALUE);

        public void reduce(IntWritable key, Iterable<Text> users,
                           Context context
        ) throws IOException, InterruptedException {
            int maxScore = 0;
            EfficientUser maxUser = new EfficientUser();

            for (Text text : users) {
                context.write(key, text);
                EfficientUser currentUser = EfficientDiverseSelector.getUser(text);
                if (currentUser.getScore() > maxScore) {
                    maxScore = currentUser.getScore();
                    maxUser = currentUser;
                }
            }
            context.write(maxInt, maxUser.toText());
        }
    }

    public static class GlobalMaxRetriever
            extends Reducer<IntWritable, Text, IntWritable, EfficientUser> {

        private static final IntWritable maxInt = new IntWritable(Integer.MAX_VALUE);

        public void reduce(IntWritable key, Iterable<Text> users, Context context)
                throws IOException, InterruptedException {
            if (key.equals(maxInt)) {
                int maxScore = 0;
                EfficientUser maxUser = new EfficientUser();
                for (Text text : users) {
                    EfficientUser user = EfficientDiverseSelector.getUser(text);
                    context.write(key, user);
                    int score = user.getScore();
                    if (score > maxScore) {
                        maxScore = score;
                        maxUser = user;
                    }
                }
                this.writeUserToHDFS(maxUser, context);
            } else if (key.equals(EfficientDiverseSelector.key)) {
                for (Text text : users) {
                    context.write(EfficientDiverseSelector.key, EfficientDiverseSelector.getUser(text));
                }
            }
        }

        private void writeUserToHDFS(EfficientUser maxUser, Context context) throws IOException {
            FSDataOutputStream os;
            BufferedWriter br;
            FileSystem hdfs = FileSystem.get(context.getConfiguration());

            Path maxFile = new Path("max.txt");
            if (hdfs.exists(maxFile)) {
                hdfs.delete(maxFile, true);
            }
            os = hdfs.create(maxFile);
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(maxUser.toString());
            br.close();

            Path resultsFile = new Path("results.txt");
            if (hdfs.exists(resultsFile)) {
                os = hdfs.append(resultsFile);
            } else {
                os = hdfs.create(resultsFile);
            }
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(maxUser.getId() + " " + maxUser.getScore());
            br.newLine();
            br.close();
        }
    }

    private static void runJob(String jobName, Class<?> jarClass, int blockSize,
                                 Class<? extends Mapper> mapper, Class<?> mapKey, Class<?> mapValue,
                                 Class<? extends Reducer> combinerClass,
                                 Class<? extends Reducer> reducerClass, Class<?> reduceKey, Class<?> reduceValue,
                                 int numReduceTasks, int numMapTasks, String inputPath, String outputPath) throws Exception {
        System.out.printf("Mappers: %d\nReducers: %d\n", numMapTasks, numReduceTasks);
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.maps", Integer.toString(numMapTasks));
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(blockSize));
        conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString(blockSize));
        blockSize -= blockSize % conf.getInt("dfs.bytes-per-checksum", 512);
        conf.set("dfs.blocksize", Integer.toString(blockSize));

        Job job = Job.getInstance(conf, jobName);
        job.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        job.setJarByClass(jarClass);

        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapKey);
        job.setMapOutputValueClass(mapValue);

        if (combinerClass != null) {
            job.setCombinerClass(combinerClass);
        }

        if (reducerClass != null) {
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(reduceKey);
            job.setOutputValueClass(reduceValue);
            job.setNumReduceTasks(numReduceTasks);
        } else {
            job.setNumReduceTasks(0);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
    }

    private int getSplitSize(long inputSize) {
        return (int) ((inputSize / this.numberMappers) + 1);
    }

    private void select() throws Exception {
        String[] jobsPaths = new String[this.selectionSize + 1];
        jobsPaths[0] = "outputGenerate";
        for (int i = 1; i < jobsPaths.length; i++) {
            jobsPaths[i] = "outputSelect" + i;
        }

        FileSystem hdfs = FileSystem.get(new Configuration());

        /* JOB 1 */
        Path inputPath = new Path(Path.CUR_DIR + "/" + this.inputPath + "/" + "groups.txt");
        long inputSize = 0;
        if (hdfs.exists(inputPath)) {
            inputSize = hdfs.getContentSummary(inputPath).getLength();
        }
        // System.out.printf("inputSize: %d, splitSize: %d\n", inputSize, this.getSplitSize(inputSize));
        EfficientDiverseSelector.runJob("Generate Users",
                EfficientDiverseSelector.class, this.getSplitSize(inputSize),
                GroupsRetriever.class, Text.class, Text.class,
                null,
                ReduceToUser.class, IntWritable.class, EfficientUser.class,
                this.numberReducers, this.numberMappers, this.inputPath, jobsPaths[0]);
        for (int i = 0; (i + 1) < jobsPaths.length; i++) {
            /* JOB 2 */
            inputSize = 0;
            for (int j = 0; j < this.numberReducers; j++) {
                inputPath = new Path(Path.CUR_DIR + "/" + jobsPaths[i] + "/" + "part-r-0000" + j);
                if (hdfs.exists(inputPath)) {
                    inputSize += hdfs.getContentSummary(inputPath).getLength();
                }
            }
            System.out.printf("inputSize: %d, splitSize: %d\n", inputSize, this.getSplitSize(inputSize));
            EfficientDiverseSelector.runJob("Find Maximal User",
                    EfficientDiverseSelector.class, this.getSplitSize(inputSize),
                    ScoreCalculator.class, IntWritable.class, Text.class,
                    LocalMaxRetriever.class,
                    GlobalMaxRetriever.class, IntWritable.class, EfficientUser.class,
                    this.numberReducers, this.numberMappers, jobsPaths[i], jobsPaths[i + 1]);
        }
    }

    public static void main(String[] args) throws Exception {
        EfficientDiverseSelector diverseSelector = new EfficientDiverseSelector();
        JCommander.newBuilder().addObject(diverseSelector).build().parse(args);
        diverseSelector.setNumberReducers();
        diverseSelector.select();
    }
}
