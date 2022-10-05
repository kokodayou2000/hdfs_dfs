package org.example.hadoop.mapreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MyWordCount {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //加载默认配置
        Configuration conf = new Configuration(true);
        //相当于客户端的实例
        Job job = Job.getInstance(conf);
        //通过传入当前类，反向推出这个类所在jar包
        job.setJarByClass(MyWordCount.class);

        job.setJobName("deng");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job,infile);

        Path outfile = new Path("/data/wc/output");
        //如果存在的话就删除这个文件
        //这个只是为了测试
        if(outfile.getFileSystem(conf).exists(outfile)){
            outfile.getFileSystem(conf).delete(outfile,true);
        }
        //设置输出路径
        TextOutputFormat.setOutputPath(job,outfile);

        //计算向数据移动
        //设置进行map的类
        job.setMapperClass(MyMapper.class);
        //set output <Type>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置进行reduce的类
        job.setReducerClass(MyReduce.class);


        //提交job，等待job结束
        job.waitForCompletion(true);

    }
}
