import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
    }

    public static class TextArrayArrayWritable extends ArrayWritable {
        public TextArrayArrayWritable() {
            super(TextArrayWritable.class);
        }

        public TextArrayArrayWritable(TextArrayWritable[] arrays) {
            super(TextArrayWritable.class);
            super.set(arrays);
        }

        @Override
        public String toString() {
            Writable[] data = super.get();
            if (data.length == 0) {
                return "";
            }

            StringBuilder sb = new StringBuilder();
            for (Writable w : data) {
                sb.append(w.toString()).append("$");
            }
            sb.setLength(sb.length() - 1);
            return sb.toString();
        }
    }

    public static class GroupsRetriever
            extends Mapper<Object, Text, Text, TextArrayWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] groups =  value.toString().split(",\\s+");
            for (String group : groups) {
                context.write(new Text(group), new TextArrayWritable(groups));
            }
        }
    }

    public static class ReduceToArray
            extends Reducer<Text, TextArrayWritable, Text, TextArrayArrayWritable> {

        public void reduce(Text user, Iterable<TextArrayWritable> groups,
                           Context context
        ) throws IOException, InterruptedException {
            List<TextArrayWritable> l = new ArrayList<TextArrayWritable>();

            for (TextArrayWritable t : groups) {
                l.add(t);
            }

            TextArrayWritable[] groupsAsArray = new TextArrayWritable[l.size()];
            l.toArray(groupsAsArray);
            context.write(user, new TextArrayArrayWritable(groupsAsArray));
        }
    }

//    public static class ScoresRetriever
//            extends Mapper<Text, List, Text, List> {
//
//        public void map(Text user, List group, Context context
//        ) throws IOException, InterruptedException {
//            int score = 0;
//        }
//    }

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
        /* JOB 1 */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Retrieve Groups");

        job.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");

        job.setJarByClass(DiverseSelector.class);
        job.setMapperClass(GroupsRetriever.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

//        job.setCombinerClass(ReduceToList.class);
        job.setReducerClass(ReduceToArray.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayArrayWritable.class);


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