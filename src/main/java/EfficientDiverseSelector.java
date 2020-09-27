import java.io.*;
import java.util.ArrayList;
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

public class EfficientDiverseSelector {

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

    public static EfficientUser GetUser(Text userText) {
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
            String group = rawGroup[0] + ", " + (rawGroup.length - 1);
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
            context.write(key, new EfficientUser(user.toString(), groupsAsArray));
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
            String[] groups = userInformation[3].split("\\s+");
            context.write(numOfReduceTasks, new Text(new EfficientUser(id, score, groups).toString()));
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
                String[] userInformation = text.toString().split("\\s+", 3);
                String currentUser = userInformation[0];
                int currentScore = Integer.parseInt(userInformation[1]);
                context.write(key, text);
                if (currentScore > maxScore) {
                    maxScore = currentScore;
                    String[] groups = userInformation[2].split("\\s+");
                    maxUser = new EfficientUser(currentUser, currentScore, groups);
                }
            }
            context.write(maxInt, new Text(maxUser.toString()));
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
                    EfficientUser user = EfficientDiverseSelector.GetUser(text);
                    int score = user.getScore();
                    if (score > maxScore) {
                        maxScore = score;
                        maxUser = user;
                    }
                }
                try {
                    this.writeUserToHDFS(maxUser.toString(), context);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                for (Text text : users) {
                    context.write(MaxUserRetriever.key, EfficientDiverseSelector.GetUser(text));
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
            extends Mapper<Object, Text, IntWritable, EfficientUser> {

        private final static IntWritable key = new IntWritable(1);
        private EfficientUser maxUser;
        private List<String> maxUserGroups;

        protected void setup(Context context) throws IOException {
            this.maxUser = readUserFromHDFS(context);
            this.maxUserGroups = new ArrayList<String>();
            for (Text group : this.maxUser.get()) {
                this.maxUserGroups.add(group.toString().split("\\s+")[1]);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueAsString = value.toString();
            value = new Text(valueAsString.substring(valueAsString.indexOf('\t') + 1));
            EfficientUser user = EfficientDiverseSelector.GetUser(value);
            List<String> newGroupsList = new ArrayList<String>();
            if (!this.maxUser.getId().equals(user.getId())) {
                for (Text group : user.get()) {
                    int groupSize = Integer.parseInt(group.toString().split("\\s+")[0]);
                    String groupId = group.toString().split("\\s+")[1];
                    if (this.maxUserGroups.contains(groupId)) {
                        groupSize -= 1;
                        if (groupSize > 0) {
                            newGroupsList.add((groupSize - 1) + " " + groupId);
                        }
                    } else {
                        newGroupsList.add(groupSize + " " + groupId);
                    }
                }
                String[] newGroupsArray = new String[newGroupsList.size()];
                context.write(MaxUserRemover.key, new EfficientUser(user.getId(), newGroupsArray));
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
                return EfficientDiverseSelector.GetUser(new Text(maxUser));
            } else {
                return null;
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
        boolean result = EfficientDiverseSelector.runJob("Retrieve Groups", DiverseSelector.class,
                GroupsRetriever.class, Text.class, Text.class,
                null,
                ReduceToUser.class, IntWritable.class, EfficientUser.class,
                inputPath, jobsPaths[0]);

        if (!result) {
            System.exit(1);
        }

        for (int i = 0; (i + 2) < jobsPaths.length; i += 2) {
            /* JOB 2 */
            result = EfficientDiverseSelector.runJob("Retrieve Maximal User", DiverseSelector.class,
                    UserRetriever.class, IntWritable.class, Text.class,
                    PartialMaxUserRetriever.class,
                    MaxUserRetriever.class, IntWritable.class, EfficientUser.class,
                    jobsPaths[i], jobsPaths[i + 1]);

            if (!result) {
                System.exit(1);
            }


            /* JOB 3 */
            result = EfficientDiverseSelector.runJob("Remove Maximal User", DiverseSelector.class,
                    MaxUserRemover.class, IntWritable.class, EfficientUser.class,
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
