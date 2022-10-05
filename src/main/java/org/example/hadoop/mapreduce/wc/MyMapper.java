package org.example.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class MyMapper extends Mapper<Object, Text,Text, IntWritable>  {
    //在hadoop框架中，它是一个分布式 数据有序列化和反序列化
    //在hadoop中就对基础数据类型进行了包装
    /*
      String --> Text
      Int --> IntWritable
      Double --> DoubleWritable
      .....
      也可以自己开发，但是必须实现序列化和反序列化还要实现比较器接口
     */
    //排序 --> 比较， 这个世界上有两种顺序： 1.字典序 2.数值顺序

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //hello hadoop 1
    //hello hadoop 2
    //TextInputFormat
    //key 是每一行字符串自己第一个字节面向源文件的偏移量
    //Value 实际上才是字符串（hello hadoop ..）

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //StringTokenizer会根据制表符，空格，换行符作为切割的依据
        //返回一个迭代器，这个迭代器就是根据分隔符分隔的集合
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()){
            //获取迭代器中的值，赋值给word中
            word.set(itr.nextToken());
            //将结果返回
            context.write(word,one);
        }
    }
}

