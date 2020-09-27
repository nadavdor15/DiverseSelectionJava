import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiverseSelector {

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

        public int getSize() {
            return super.get().length;
        }

        @Override
        public String toString() {
            Writable[] data = super.get();
            if (data.length == 0) {
                return "";
            }

            StringBuilder sb = new StringBuilder();
            for (Writable w : data) {
                sb.append(w.toString()).append(", ");
            }
            sb.setLength(sb.length() - 2);
            return sb.toString();
        }

        protected TextArrayWritable clone() {
            TextArrayWritable textArrayWritable = new TextArrayWritable();
            textArrayWritable.set(super.get().clone());
            return textArrayWritable;
        }
    }

    public static class GroupsRetriever
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] group = value.toString().split(",\\s+");
            for (String user : group) {
                context.write(new Text(user), new TextArrayWritable(group));
            }
        }
    }

    public static class ReduceToUser
            extends Reducer<Text, TextArrayWritable, IntWritable, User> {

        private final static IntWritable key = new IntWritable(1);

        public void reduce(Text user, Iterable<TextArrayWritable> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<TextArrayWritable> groupsAsList = new ArrayList<TextArrayWritable>();

            for (TextArrayWritable textArrayWritable : groups) {
                groupsAsList.add(textArrayWritable.clone());
            }

            TextArrayWritable[] groupsAsArray = new TextArrayWritable[groupsAsList.size()];
            groupsAsList.toArray(groupsAsArray);
            context.write(key, new User(user.toString(), groupsAsArray));
        }
    }

    public static class UserRetriever
            extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable numOfReduceTasks;

        protected void setup(Context context) {
            this.numOfReduceTasks = new IntWritable(context.getNumReduceTasks());
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] userInformation = value.toString().split("\\s+", 4);
            String id = userInformation[1];
            int score = Integer.parseInt(userInformation[2]);
            String[] rawGroups = userInformation[3].split("[$]");
            TextArrayWritable[] groups = new TextArrayWritable[rawGroups.length];
            for (int i = 0; i < rawGroups.length; i++) {
                groups[i] = new TextArrayWritable(rawGroups[i].split(",\\s+"));
            }
            context.write(numOfReduceTasks, new Text(new User(id, score, groups).toString()));
        }
    }

    public static class PartialMaxUserRetriever
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private static final IntWritable maxInt = new IntWritable(Integer.MAX_VALUE);

        public void reduce(IntWritable key, Iterable<Text> groups,
                           Context context
        ) throws IOException, InterruptedException {
            int maxScore = 0;
            String maxUser = "";

            for (Text text : groups) {
                String[] userInformation = text.toString().split("\\s+", 3);
                String currentUser = userInformation[0];
                int currentScore = Integer.parseInt(userInformation[1]);
                context.write(key, text);
                if (currentScore > maxScore) {
                    maxScore = currentScore;
                    maxUser = currentUser;
                }
            }
            context.write(maxInt, new Text(maxUser + " " + maxScore));
        }
    }

    public static class MaxUserRetriever
            extends Reducer<IntWritable, Text, IntWritable, User> {

        private static final IntWritable key = new IntWritable(1);
        private static final IntWritable maxInt = new IntWritable(Integer.MAX_VALUE);

        public void reduce(IntWritable key, Iterable<Text> groups,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.equals(maxInt)) {
                int maxScore = 0;
                String maxUser = "";
                for (Text text : groups) {
                    String[] userInformation = text.toString().split("\\s+", 2);
                    String currentUser = userInformation[0];
                    int currentScore = Integer.parseInt(userInformation[1]);
                    if (currentScore > maxScore) {
                        maxScore = currentScore;
                        maxUser = currentUser;
                    }
                }
                try {
                    this.writeUserToHDFS(maxUser + " " + maxScore, context);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                for (Text text : groups) {
                    String[] userInformation = text.toString().split("\\s+", 3);
                    String currentUser = userInformation[0];
                    int currentScore = Integer.parseInt(userInformation[1]);
                    String[] rawGroups = userInformation[2].split("[$]");
                    TextArrayWritable[] groupsAsArray = new TextArrayWritable[rawGroups.length];
                    for (int i = 0; i < rawGroups.length; i++) {
                        groupsAsArray[i] = new TextArrayWritable(rawGroups[i].split(",\\s+"));
                    }
                    context.write(MaxUserRetriever.key, new User(currentUser, currentScore, groupsAsArray));
                }
            }
        }

        private void writeUserToHDFS(String maxUser, Context context) throws IOException {
            FSDataOutputStream os;
            BufferedWriter br;
            FileSystem hdfs = FileSystem.get(context.getConfiguration());

            Path maxFile = new Path("max.txt");
            if (hdfs.exists(maxFile)) {
                hdfs.delete(maxFile, true);
            }
            os = hdfs.create(maxFile);
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(maxUser);
            br.close();

            Path resultsFile = new Path("results.txt");
            if (hdfs.exists(resultsFile)) {
                os = hdfs.append(resultsFile);
            } else {
                os = hdfs.create(resultsFile);
            }
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write(maxUser);
            br.newLine();
            br.close();

            hdfs.close();
        }
    }

    public static class MaxUserRemover
            extends Mapper<Object, Text, IntWritable, User> {

        private final static IntWritable key = new IntWritable(1);
        private String maxUser;

        protected void setup(Context context) throws IOException {
            String maxUserInformation = readUserFromHDFS(context);
            this.maxUser = maxUserInformation.split("\\s+")[0];
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] userInformation = value.toString().split("\\s+", 4);
            String id = userInformation[1];
            if (!this.maxUser.equals(id)) {
                int score = Integer.parseInt(userInformation[2]);
                String[] rawGroups = userInformation[3].split("[$]");
                TextArrayWritable[] groups = new TextArrayWritable[rawGroups.length];
                for (int i = 0; i < rawGroups.length; i++) {
                    List<String> groupAsList =
                            new ArrayList<String>(Arrays.asList(rawGroups[i].split(",\\s+")));
                    groupAsList.remove(this.maxUser);
                    String[] groupWithoutMax = new String[groupAsList.size()];
                    groupAsList.toArray(groupWithoutMax);
                    groups[i] = new TextArrayWritable(groupWithoutMax);
                }
                context.write(MaxUserRemover.key, new User(id, groups));
            }
        }

        private String readUserFromHDFS(Context context) throws IOException {
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
                return maxUser;
            } else {
                return "";
            }
        }
    }

    public static boolean runJob(String jobName, Class<?> jarClass,
                                 Class<? extends Mapper> mapper, Class<?> mapKey, Class<?> mapValue,
                                 Class<? extends Reducer> combinerClass,
                                 Class<? extends Reducer> reducerClass, Class<?> reduceKey, Class<?> reduceValue,
                                 String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("mapred.max.split.size", "67108864");

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
            job.setNumReduceTasks(14);
        } else {
            job.setNumReduceTasks(0);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        int selectionSize = Integer.parseInt(args[1]);

        String[] jobsPaths = new String[selectionSize * 2 + 1];

        jobsPaths[0] = "outputJob1";

        for (int i = 1; i < jobsPaths.length; i++) {
            if (i % 2 == 0) {
                jobsPaths[i] = "outputFind" + i;
            } else {
                jobsPaths[i] = "outputRemove" + i;
            }
        }

        /* JOB 1 */
        boolean result = DiverseSelector.runJob("Retrieve Groups", DiverseSelector.class,
                GroupsRetriever.class, Text.class, TextArrayWritable.class,
                null,
                ReduceToUser.class, IntWritable.class, User.class,
                inputPath, jobsPaths[0]);

        if (!result) {
            System.exit(1);
        }

        for (int i = 0; (i + 2) < jobsPaths.length; i += 2) {
            /* JOB 2 */
            result = DiverseSelector.runJob("Retrieve Maximal User", DiverseSelector.class,
                    UserRetriever.class, IntWritable.class, Text.class,
                    PartialMaxUserRetriever.class,
                    MaxUserRetriever.class, IntWritable.class, Text.class,
                    jobsPaths[i], jobsPaths[i + 1]);

            if (!result) {
                System.exit(1);
            }


            /* JOB 3 */
            result = DiverseSelector.runJob("Remove Maximal User", DiverseSelector.class,
                    MaxUserRemover.class, IntWritable.class, User.class,
                    null,
                    null, null, null,
                    jobsPaths[i + 1], jobsPaths[i + 2]);

            if (!result) {
                System.exit(1);
            }
        }

        System.exit(0);
    }
}