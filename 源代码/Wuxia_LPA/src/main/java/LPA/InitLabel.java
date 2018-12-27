package LPA;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InitLabel {
    public static class Map extends Mapper<Object, Text, Text, Text> {

        Path prPath = new Path("./result/part-r-00000");
        Path labelPath = new Path("./temp/label.txt");

        List<String> labels = new ArrayList<String>();
        Hashtable<String, String> keyList = new Hashtable<String, String>();

        protected void setup(Context context) throws IOException {
            // 读取数据
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fin = fs.open(prPath);
            BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));

            for(int i = 0; i < 15; i++){
                String line = input.readLine();
                String label = line.split("\t")[1];
                labels.add(label);
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("part")){
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    String line = itr.nextToken();
                    String keyName = line.split("\t")[0];
                    String links = line.split("\t")[1];
                    if(labels.contains(keyName)) {
                        context.write(new Text(keyName), new Text(keyName + links));
                        keyList.put(keyName, keyName);
                    }
                    else {
                        context.write(new Text(keyName), new Text("None"+links));
                        keyList.put(keyName, "None");
                    }
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream fin = fs.create(labelPath, true);
            Enumeration l = keyList.keys();
            while(l.hasMoreElements()){
                String key = (String)l.nextElement();
                String label = keyList.get(key);
                String line = key + "\t" + label + "\n";
                fin.write(line.getBytes(), 0, line.getBytes().length);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Init Label");
        job.setJarByClass(InitLabel.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
