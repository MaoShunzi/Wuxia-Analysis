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

public class LPA {
    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("part")){
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    String line = itr.nextToken();
                    String keyName = line.split("\t")[0];
                    String labelLink = line.split("\t")[1];
                    context.write(new Text(keyName), new Text(labelLink));
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        Path prPath = new Path("./result/part-r-00000");
        Path labelPath = new Path("./temp/label.txt");
        List<String> labels = new ArrayList<String>();

        Hashtable<String, Double> neighborLabel = new Hashtable<String, Double>();
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

            fin.close();
            fin = fs.open(labelPath);
            input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
            String s;
            while ((s = input.readLine()) != null) {
                String[] items = s.split("\t");
                keyList.put(items[0], items[1]);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            neighborLabel.clear();
            int index = labels.indexOf(key.toString());
            if(index != -1){
                if(!keyList.containsKey(key.toString()))
                    keyList.put(key.toString(), labels.get(index));
            }
            else {
                for(Text value:values) {
                    String linkNames = value.toString().substring(value.toString().indexOf("[")+1, value.toString().length()-1);
                    String lastLabel = value.toString().split("\\[")[0];
                    //String linkNames = line.split("\\[")[1];

                    String[] links = linkNames.split("\\|");
                    for (int i = 0; i < links.length; i++) {
                        String linkName = links[i].split(",")[0];
                        Double linkWeight = Double.parseDouble(links[i].split(",")[1]);
                        String linkLabel = keyList.get(linkName);
                        if(labels.indexOf(linkLabel) != -1){
                            if(neighborLabel.get(linkLabel) == null)
                                neighborLabel.put(linkLabel, linkWeight);
                            else{
                                Double labelValue = neighborLabel.get(linkLabel) + linkWeight;
                                neighborLabel.remove(linkLabel);
                                neighborLabel.put(linkLabel, labelValue);
                            }
                        }
                    }
                    Enumeration l = neighborLabel.keys();
                    Double maxValue = 0.0;
                    String finalLabel = lastLabel;
                    while(l.hasMoreElements()){
                        String label = (String)l.nextElement();
                        if(neighborLabel.get(label) > maxValue){
                            maxValue = neighborLabel.get(label);
                            finalLabel = label;
                        }
                    }
                    keyList.remove(key.toString());
                    keyList.put(key.toString(), finalLabel);
                    context.write(new Text(key.toString()), new Text(finalLabel + "[" + linkNames + "]"));
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
        Job job = Job.getInstance(conf, "LPA");
        job.setJarByClass(LPA.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
