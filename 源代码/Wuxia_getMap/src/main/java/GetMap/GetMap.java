package GetMap;

import java.io.IOException;
import java.util.*;

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

public class GetMap {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text namePair = new Text();
        private Text Count = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //GetFeature的结果文件是part-r-00000
            if(fileName.contains("part")){
                StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
                while(itr.hasMoreTokens()) {
                    String names = itr.nextToken();
                    //用来分割每一行的index
                    int splitName1 = names.indexOf(",");
                    int splitName2 = names.indexOf(">");
                    int splitCount = names.indexOf("\t");

					//同现对的第一个人名
                    String name1 = names.substring(1, splitName1);
                    //同现对的第二个人名
                    String name2 = names.substring(splitName1+1, splitName2);
                    //同现次数
                    int count = Integer.parseInt(names.substring(splitCount+1));

                    namePair.set(name1 + "#" + name2);
                    Count.set(Integer.toString(count));
                    context.write(namePair, Count);
                }
            }
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	//保存lastKeyName链接向的人名
        private List<String> nameList = new ArrayList<String>();
        //保存lastKeyName与nameList中每个人名的同现次数
        private List<Integer> nameCount = new ArrayList<Integer>();
        //保存lastKeyName与nameList中所有人名的总同现次数，用来归一化
        private int countSum = 0;
        //保存当前的KeyName
        private String curKeyName = new String();
        //保存上一个KeyName
        private String lastKeyName = "NULL";
        //保存"[name1,weight1|name2,weight2|...]"
        private StringBuilder neighborList = new StringBuilder();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

			//从key中获取当前keyName和与它同现的name
            curKeyName = key.toString().split("#")[0];
            String curName = key.toString().split("#")[1];
			
			//如果当前keyName和上一个keyName相同或是第一个keyName，继续统计
            if(curKeyName.equals(lastKeyName) || lastKeyName.equals("NULL")){
                int count = 0;
                for(Text value : values)
                    count += Integer.parseInt(value.toString());
                //总同现次数
                countSum += count;
                //把与keyName同现到name和count加入
                nameList.add(curName);
                nameCount.add(count);
            }
            //如果当前keyName和上一个keyName不同，表示上一个keyName的统计完成
            else {
            	//返回结果
                neighborList.append("[");
                ListIterator nameItr = nameList.listIterator();
                ListIterator countItr = nameCount.listIterator();
                while(nameItr.hasNext() && countItr.hasNext()){
                    String name = (String)nameItr.next();
                    int count = (Integer)countItr.next();
                    neighborList.append(name+","+(float)count/countSum+"|");
                }
                neighborList.deleteCharAt(neighborList.lastIndexOf("|"));
                neighborList.append("]");
                context.write(new Text(lastKeyName), new Text(neighborList.toString()));

				//初始化并进行当前keyName统计
                nameList = new ArrayList<String>();
                nameCount = new ArrayList<Integer>();
                neighborList = new StringBuilder();
                countSum = 0;

                int count = 0;
                for(Text value : values)
                    count += Integer.parseInt(value.toString());
                countSum += count;
                nameList.add(curName);
                nameCount.add(count);
            }
            lastKeyName = curKeyName;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
			//返回最后一行的结果
            neighborList.append("[");
            ListIterator nameItr = nameList.listIterator();
            ListIterator countItr = nameCount.listIterator();
            while(nameItr.hasNext() && countItr.hasNext()){
                String name = (String)nameItr.next();
                int count = (Integer)countItr.next();
                neighborList.append(name+","+(float)count/countSum+"|");
            }
            neighborList.deleteCharAt(neighborList.lastIndexOf("|"));
            neighborList.append("]");
            context.write(new Text(curKeyName), new Text(neighborList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Get Map");
        job.setJarByClass(GetMap.class);
        job.setMapperClass(Map.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
