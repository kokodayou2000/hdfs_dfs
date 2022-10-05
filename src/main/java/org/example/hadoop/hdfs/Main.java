package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class Main {

    static Configuration conf = null;
    static FileSystem fs = null;



    public static void main(String[] args) throws IOException {
        conf = new Configuration(true);
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");


        fs = FileSystem.get(conf);
        Path dir  = new Path("/tom");
        fs.mkdirs(dir);

        fs.close();


    }
}
