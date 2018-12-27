package GetFeature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Iterator;

import com.sun.org.apache.xerces.internal.xs.StringList;
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

public class GetFeature {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text namePair = new Text();
        private Text Count = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //获取文件名
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            
			//如果是金庸的小说
            if(fileName.contains("金庸")) {
            	//获取每一段，即用换行符分割文件内容
                StringTokenizer paraItr = new StringTokenizer(value.toString(),"\n");
                while (paraItr.hasMoreTokens()) {
                    String paraString = paraItr.nextToken();
                    //获取每一段中到人物名，即用空格分割每一段内容
                    StringTokenizer wordItr = new StringTokenizer(paraString," ");
                    //已经存在到同现对，用来判断是否重复计算
                    List<String> existPairs = new ArrayList<String>();
                    //对每一段的人名两两配对<word1,word2>
                    while (wordItr.hasMoreTokens()){
                        String word1 = wordItr.nextToken();
                        StringTokenizer wordItr2 = wordItr;
                        while (wordItr2.hasMoreTokens()){
                            String word2 = wordItr2.nextToken();
                            //如果word1 != word2，即是同现对
                            if(!word1.equals(word2)){
                                String pair1 = "<"+word1+","+word2+">";
                                String pair2 = "<"+word2+","+word1+">";
                                //如果同一段中该同现对未被计算过
                                if(existPairs.indexOf(pair1) == -1) {
                                    existPairs.add(pair1);
                                    namePair.set(pair1);
                                    //同现次数为1
                                    Count.set("1");
                                    context.write(namePair, Count);
                                }
                                if(existPairs.indexOf(pair2) == -1) {
                                    existPairs.add(pair2);
                                    namePair.set(pair2);
                                    Count.set("1");
                                    context.write(namePair, Count);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text Count = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //将相同同现对的同现次数累加
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            Count.set(Integer.toString(sum));
            context.write(key, Count);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Feature");
        job.setJarByClass(GetFeature.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
