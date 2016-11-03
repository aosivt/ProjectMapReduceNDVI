package aosivt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;



/**
 * Created by alex on 25.09.15.
 */
public class GetBinMap {
    static final Logger logger = Logger.getLogger(GetBinMap.class);
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "CountKey");
        job.setJarByClass(GetBinMap.class);
        job.setMapperClass(TestGetInfoBInMapper.class);
        job.setReducerClass(TestGetInfoBInReducer.class);

        job.setOutputKeyClass(HaLongWritable.class);
        job.setGroupingComparatorClass(Cmprs.class );
        //job.setPartitionerClass(RecPartitioner.class);
        ///job.setNumReduceTasks( 7 );

        job.setMapOutputValueClass(ObjectWritableBin.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass( Sortulka.class );
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }



}

