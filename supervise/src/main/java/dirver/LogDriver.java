package dirver;

import mapper.LogMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import output.FilterOutputFormat;
import reducer.LogReducer;

import java.io.IOException;

/**
 * mapreduce驱动类
 * 输入两个参数：
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 3){
            System.out.println("Usage:<in> <out> <dt>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns");//指定hdfs的nameservice为cluster1,是NameNode的URI
        conf.set("dfs.nameservices", "ns");//指定hdfs的nameservice为cluster1
        conf.set("dfs.ha.namenodes.ns", "nna,nns");//cluster1下面有两个NameNode，分别是nna，nns
        conf.set("dfs.namenode.rpc-address.ns.nna", "10.10.10.122:8020");//nna的RPC通信地址
        conf.set("dfs.namenode.rpc-address.ns.nns", "10.10.10.123:8020");//nns的RPC通信地址
        conf.set("dfs.client.failover.proxy.provider.ns", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");//配置失败自动切换实现方式

        String inputPath = args[0]; //输入路径
        String localTestPath = args[1]; //输出路径
        String time = args[2]; //指定目录日期
        String preoutputPath = args[1]; //success文件存放位置
        long tmpTime = System.currentTimeMillis();
        String tmpPath = "/tmp/defaultoutPut/"+tmpTime;

        conf.set("parseLogTime", time);
        conf.set("preoutputPath", preoutputPath);

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        // 要将自定义的输出格式组件设置到 job 中
        job.setOutputFormatClass(FilterOutputFormat.class);

//        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop1:8020/tmp/supervise.log"));
        FileInputFormat.setInputPaths(job, new Path(inputPath));

//        FileOutputFormat.setOutputPath(job, new Path(localTestPath));
        FileOutputFormat.setOutputPath(job, new Path(tmpPath));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
