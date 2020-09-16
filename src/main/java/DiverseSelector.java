import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.Progressable;

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
            String[] group =  value.toString().split(",\\s+");
            for (String user : group) {
                context.write(new Text(user), new TextArrayWritable(group));
            }
        }
    }

    public static class ReduceToUser
            extends Reducer<Text, TextArrayWritable, Text, User> {

        public void reduce(Text user, Iterable<TextArrayWritable> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<TextArrayWritable> groupsAsList = new ArrayList<TextArrayWritable>();

            for (TextArrayWritable textArrayWritable : groups) {
                groupsAsList.add(textArrayWritable.clone());
            }

            TextArrayWritable[] groupsAsArray = new TextArrayWritable[groupsAsList.size()];
            groupsAsList.toArray(groupsAsArray);
            context.write(user, new User(user.toString(), groupsAsArray));
        }
    }

    public static class UserRetriever
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] userInformation = value.toString().split("\\s+", 3);
            Text user = new Text(userInformation[0]);
            int score = Integer.parseInt(userInformation[1]);
            String[] rawGroups = userInformation[2].split("[$]");
            TextArrayWritable[] groups = new TextArrayWritable[rawGroups.length];
            for (int i = 0; i < rawGroups.length; i++) {
                groups[i] = new TextArrayWritable(rawGroups[i].split(",\\s+"));
            }
            context.write(one, new Text(user.toString() + " " + new User(user.toString(), score, groups).toString()));
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

        private void writeMaxToHDFS(String maxUser) throws Exception {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
            Path file = new Path("hdfs://localhost:9000/stam/max.txt");
            if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
            OutputStream os = hdfs.create( file, new Progressable() { public void progress() { } });
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8" ) );
            br.write(maxUser);
            br.close();
            hdfs.close();
        }
    }

    public static class MaxUserRetriever
            extends Reducer<IntWritable, Text, Text, User> {

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
                    context.write(new Text(currentUser), new User(currentUser, currentScore, groupsAsArray));
                }
            }
        }

        private void writeUserToHDFS(String maxUser, Context context) throws IOException {
            FSDataOutputStream os = null;
            BufferedWriter br = null;
            FileSystem hdfs = FileSystem.get(context.getConfiguration());

            Path maxFile = new Path("max.txt");
            if (hdfs.exists(maxFile)) {
                hdfs.delete(maxFile, true );
            }
            os = hdfs.create(maxFile);
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8" ) );
            br.write(maxUser);
            br.close();

            Path resultsFile = new Path("results.txt");
            if (hdfs.exists(resultsFile)) {
                os = hdfs.append(resultsFile);
            } else {
                os = hdfs.create(resultsFile);
            }
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8" ) );
            br.write(maxUser);
            br.newLine();
            br.close();

            hdfs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        /* JOB 1 */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Retrieve Groups");

        job.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        job.setJarByClass(DiverseSelector.class);
        job.setMapperClass(GroupsRetriever.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setReducerClass(ReduceToUser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(User.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        if (!result) {
            System.exit(1);
        }

        /* JOB 2 */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Retrieve Maximal User");

        job2.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        job2.setJarByClass(DiverseSelector.class);
        job2.setMapperClass(UserRetriever.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setCombinerClass(PartialMaxUserRetriever.class);
        job2.setReducerClass(MaxUserRetriever.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        result = job2.waitForCompletion(true);

        if (!result) {
            System.exit(1);
        }

        System.exit( 0);
    }
}