import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
    int selectionSize = 0;
    @Parameter(names={"-i", "--input"}, description = "Path of the groups.txt file")
    String inputPath = "input";
    @Parameter(names={"-m", "--mappers"}, description = "Number of mappers")
    int numberMappers = 8;
    @Parameter(names={"-r", "--reducers"}, description = "Proportion of reducers to mappers")
    double reducersProportion = 1.75;
    private int numberReducers;

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
        this.numberReducers = (int) (this.reducersProportion > 0 ?
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

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rawGroup = value.toString().split(",\\s+");
            String group = rawGroup[0] + "," + (rawGroup.length - 1);
            for (int i = 1; i < rawGroup.length; i++) {
                context.write(new Text(rawGroup[i]), new Text(group));
            }
        }
    }

    public static class ReduceToUser
            extends Reducer<Text, Text, IntWritable, EfficientUser> {

        private final static IntWritable key = new IntWritable(1);

        public void reduce(Text user, Iterable<Text> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> groupsAsList = new ArrayList<String>();

            for (Text group : groups) {
                groupsAsList.add(group.toString());
            }

            String[] groupsAsArray = new String[groupsAsList.size()];
            context.write(key, new EfficientUser(user.toString(), groupsAsList.toArray(groupsAsArray)));
        }
    }

    public static class UserRetriever
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable randomKey;

        protected void setup(Context context) {
            Random rand = new Random();
            this.randomKey = new IntWritable(rand.nextInt(context.getNumReduceTasks()));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueAsString = value.toString();
            String userInformation = valueAsString.substring(valueAsString.indexOf('\t') + 1);
            context.write(randomKey, new Text(userInformation));
        }
    }

    public static class PartialMaxUserRetriever
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

    public static class MaxUserRetriever
            extends Reducer<IntWritable, Text, IntWritable, EfficientUser> {

        private static final IntWritable key = new IntWritable(1);
        private static final IntWritable maxInt = new IntWritable(Integer.MAX_VALUE);

        public void reduce(IntWritable key, Iterable<Text> users,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.equals(maxInt)) {
                int maxScore = 0;
                EfficientUser maxUser = new EfficientUser();
                for (Text text : users) {
                    EfficientUser user = EfficientDiverseSelector.getUser(text);
                    int score = user.getScore();
                    if (score > maxScore) {
                        maxScore = score;
                        maxUser = user;
                    }
                }
                try {
                    this.writeUserToHDFS(maxUser, context);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                for (Text text : users) {
                    context.write(MaxUserRetriever.key, EfficientDiverseSelector.getUser(text));
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

            hdfs.close();
        }
    }

    public static class MaxUserRemover
            extends Mapper<Object, Text, IntWritable, EfficientUser> {

        private final static IntWritable key = new IntWritable(1);
        private EfficientUser maxUser;
        private List<String> maxUserGroups;

        protected void setup(Context context) throws IOException {
            this.maxUser = readUserFromHDFS(context);
            this.maxUserGroups = new ArrayList<String>();
            for (Text group : this.maxUser.get()) {
                this.maxUserGroups.add(group.toString().split("\\s+")[0]);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueAsString = value.toString();
            value = new Text(valueAsString.substring(valueAsString.indexOf('\t') + 1));
            EfficientUser user = EfficientDiverseSelector.getUser(value);
            List<String> newGroupsList = new ArrayList<String>();
            if (!this.maxUser.getId().equals(user.getId())) {
                for (Text group : user.get()) {
                    String[] groupInformation = group.toString().split(",");
                    String groupIdx = groupInformation[0];
                    int groupSize = Integer.parseInt(groupInformation[1]);
                    if (this.maxUserGroups.contains(groupIdx)) {
                        groupSize -= 1;
                    }
                    if (groupSize > 0) {
                        newGroupsList.add(groupIdx + "," + groupSize);
                    }
                }
                String[] newGroupsArray = new String[newGroupsList.size()];
                context.write(MaxUserRemover.key,
                        new EfficientUser(user.getId(), newGroupsList.toArray(newGroupsArray)));
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

    public static void runJob(String jobName, Class<?> jarClass, int blockSize,
                                 Class<? extends Mapper> mapper, Class<?> mapKey, Class<?> mapValue,
                                 Class<? extends Reducer> combinerClass,
                                 Class<? extends Reducer> reducerClass, Class<?> reduceKey, Class<?> reduceValue,
                                 int numReduceTasks, String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", Integer.toString(blockSize));

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

    public static void main(String[] args) throws Exception {
        EfficientDiverseSelector diverseSelector = new EfficientDiverseSelector();
        JCommander.newBuilder().addObject(diverseSelector).build().parse(args);
        diverseSelector.setNumberReducers();
        diverseSelector.select();
    }

    public void select() throws Exception {
//        System.out.printf("Input: %s\nSelection size: %d\nMappers: %d\nReducers: %d\n",
//                this.inputPath, this.selectionSize, this.numberMappers, this.numberReducers);
        String[] jobsPaths = new String[this.selectionSize * 2 + 1];

        jobsPaths[0] = "outputFirstJob";
        for (int i = 1; i < jobsPaths.length; i++) {
            if (i % 2 == 1) {
                jobsPaths[i] = "outputFind" + (i/2 + 1);
            } else {
                jobsPaths[i] = "outputRemove" + (i/2);
            }
        }

        /* JOB 1 */
        Path inputPath = new Path(Path.CUR_DIR + "/" + this.inputPath + "/" + "groups.txt");
        long inputSize = inputPath.getFileSystem(new Configuration()).getContentSummary(inputPath).getLength();
        EfficientDiverseSelector.runJob("Retrieve Groups",
                EfficientDiverseSelector.class, (int) ((inputSize / this.numberMappers) + 1),
                GroupsRetriever.class, Text.class, Text.class,
                null,
                ReduceToUser.class, IntWritable.class, EfficientUser.class,
                (int) (this.numberMappers * 1.75), this.inputPath, jobsPaths[0]);

        for (int i = 0; (i + 2) < jobsPaths.length; i += 2) {
            /* JOB 2 */
            EfficientDiverseSelector.runJob("Find Maximal User",
                    EfficientDiverseSelector.class, ((115910313 / this.numberMappers) + 1),
                    UserRetriever.class, IntWritable.class, Text.class,
                    PartialMaxUserRetriever.class,
                    MaxUserRetriever.class, IntWritable.class, EfficientUser.class,
                    1, jobsPaths[i], jobsPaths[i + 1]);

            /* JOB 3 */
            EfficientDiverseSelector.runJob("Remove Maximal User",
                    EfficientDiverseSelector.class, ((115910313 / this.numberMappers) + 1),
                    MaxUserRemover.class, IntWritable.class, EfficientUser.class,
                    null,
                    null, null, null,
                    (int) (this.numberMappers * 1.75), jobsPaths[i + 1], jobsPaths[i + 2]);
        }
    }
}
