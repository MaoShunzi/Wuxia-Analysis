package PageRank;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class PageRankViewer {
    public static class Map extends Mapper<Object,Text,DoubleWritable,Text> {

        private Text outName = new Text();
        private DoubleWritable outPr = new DoubleWritable();
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.contains("part")) {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while(itr.hasMoreTokens()) {
                    String[] line = itr.nextToken().toString().split("\t");
                    String keyName = line[0];
                    Double pr = Double.parseDouble(line[1]);
                    outName.set(keyName);
                    outPr.set(pr);
                    context.write(outPr, outName);
                }
            }
        }
    }

    private static class DoubleWritableDecressingComparator extends DoubleWritable.Comparator {

        public int compare(WritableComparable a,WritableComparable b) {
            return -super.compare(a,b);
        }
        public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2) {
            return -super.compare(b1,s1,l1,b2,s2,l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankViewer");
        job.setJarByClass(PageRankViewer.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(DoubleWritableDecressingComparator.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
