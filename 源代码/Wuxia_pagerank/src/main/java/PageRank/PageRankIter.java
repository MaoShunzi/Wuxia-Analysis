package PageRank;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class PageRankIter {
    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.contains("part")) {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    String[] line = itr.nextToken().split("\t");
                    //String[] line = value.toString().split("\t");
                    String keyName = line[0];
                    double prValue = Double.parseDouble(line[1]);
                    String linkNames = line[2];
                    context.write(new Text(keyName), new Text(linkNames));

                    String[] links = {""};
                    //if(linkNames.contains("|"))
                        links = linkNames.substring(1, linkNames.length() - 1).split("\\|");
                    //else links[0] = linkNames;
                    for (int i = 0; i < links.length; i++) {
                        //if(links[i].contains(",")) {
                            String linkName = links[i].split(",")[0];
                            String linkWeight = links[i].split(",")[1];
                            String linkPrValue = String.valueOf(prValue * Double.parseDouble(linkWeight));
                            //Double linkWeight = 1.0/links.length;
                            //String linkPrValue = String.valueOf(prValue * linkWeight);

                            context.write(new Text(linkName), new Text(linkPrValue));
                        //}
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String M = new String();
            double pr = 0;
            for(Text value:values){
                String valueString = value.toString();
                if(valueString.startsWith("["))
                    M = "\t" + valueString;
                else
                    pr += Double.parseDouble(valueString);
            }
            context.write(new Text(key), new Text(String.valueOf(pr)+M));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankIter");
        job.setJarByClass(PageRankIter.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}

