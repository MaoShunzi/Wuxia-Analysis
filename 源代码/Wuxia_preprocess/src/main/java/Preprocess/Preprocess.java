package Preprocess;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.nlpcn.commons.lang.tire.domain.Forest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
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


public class Preprocess {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text docName = new Text();
        private Text docPara = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            /*去除文件名后缀*/
            int splitFileName = fileName.toString().indexOf(".txt");
            //if (splitFileName == -1) splitFileName = fileName.toString().indexOf(".TXT.segmented");
            if(splitFileName > 0)
                fileName = fileName.substring(0, splitFileName);

            StringTokenizer itr = new StringTokenizer(value.toString(), "\r\n");
            while(itr.hasMoreTokens()) {
                docName.set(fileName);
                docPara.set(itr.nextToken());
				//返回(文件名, 段落)
                context.write(docName, docPara);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private String para = new String();
        private String result = new String();
		//多文件输出
        private MultipleOutputs outputs;
        Path filePath = new Path("People_List_unique.txt");
		
		//在setup中读取人名列表并加入到词典中
        protected void setup(Context context) throws IOException {
            // 读取数据
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fin = fs.open(filePath);
            BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));

			//读取每个人名加入到词典中
            String s = new String();
            while ((s = input.readLine()) != null) {
                String[] items = s.split("\n");
                for(int i = 0; i < items.length; i++)
					//词的类型是wuxia, 词频1000
                    DicLibrary.insert(DicLibrary.DEFAULT, items[i], "wuxia", 1000);
            }
            outputs = new MultipleOutputs(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            result = "\n";
            for(Text value : values) {
                para = value.toString();
				//分词
                Result parse = DicAnalysis.parse(para);
                boolean flag = false;
                for (Term term : parse) {
					//如果词的类型是wuxia, 保留
                    if (term.getNatureStr().equals("wuxia")) {
                        flag = true;
                        result = result + term.getName() + ' ';
                    }
                }
                if(flag)
                    result = result + '\n';
            }
			//输出到文件, 文件名是key, 即对应的小说文件名
            outputs.write(key, new Text(result), key.toString());
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            outputs.close();
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Preprocess");
        job.setJarByClass(Preprocess.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        /*DicLibrary.insert(DicLibrary.DEFAULT, "增加新词", "我是词性", 1000);
        Result parse = DicAnalysis.parse("这是用户自定义词典增加新词的例子");
        System.out.println(parse);*/
    }
}
