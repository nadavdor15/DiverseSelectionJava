import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiverseSelector {

    public static class GroupsRetriever
            extends Mapper<Object, Text, Text, List> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            List<String> group = Arrays.asList(value.toString().split("\\s+"));
            for (int i = 0; i < group.size(); i++) {
                context.write(new Text(group.get(i)), group);
            }
        }
    }

    public static class ReduceToList
            extends Reducer<Text, List, Text, List> {

        public void reduce(Text user, Iterable<List> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<List> groupsAsList = new ArrayList<List>();
//            groups.forEach(groupsAsList::add);
            context.write(user, groupsAsList);
        }
    }

    public static class ScoresRetriever
            extends Mapper<Text, List, Text, List> {

        public void map(Text user, List group, Context context
        ) throws IOException, InterruptedException {
            int score = 0;
        }
    }

//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        /* JOB 1 */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Retrieve Groups");

        job.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        job.setJarByClass(DiverseSelector.class);
        job.setMapperClass(GroupsRetriever.class);
        job.setReducerClass(ReduceToList.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(List.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        if (!result) {
            System.exit(1);
        }

        /* JOB 2 */
//        Configuration conf2 = new Configuration();
//        Job job2 = Job.getInstance(conf, "Name 2");
//
//        job2.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
//
//        job2.setJarByClass(DiverseSelector.class);
//        job2.setMapperClass(ScoresRetriever.class);
//        job2.setReducerClass(ReduceToList.class);
//
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(List.class);
//
//        job2.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
//
//        FileInputFormat.addInputPath(job2, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
//
//        result = job2.waitForCompletion(true);
//
//        if (!result) {
//            System.exit(1);
//        }

        System.exit(result ? 0 : 1);
    }
}