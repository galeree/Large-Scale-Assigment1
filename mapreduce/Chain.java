import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;

public class Chain extends Configured implements Tool
{
    public static String path = "/Users/Galle/Large-Scale-Assigment1/";
    
	@Override
	public int run(String[] arg0) throws Exception {
		String[] job1 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2" ,
						"-mapper"   , path+"mapreduce1.py mapper" ,
				        "-reducer"  , path+"mapreduce1.py reducer" ,
				        "-input"    , "/input/*" ,
				        "-output"   , "/output" ,
				};
				        
		JobConf job1Conf = StreamJob.createJob( job1);
		JobClient.runJob( job1Conf);
		
		String[] job2 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2nr" ,
						"-mapper"   , path+"mapreduce2.py mapper" ,
			            "-reducer"  , path+"mapreduce2.py reducer" ,
			            "-input"    , "/output/part-00000" ,
			            "-output"   , "/sort" ,
				};
				        
		JobConf job2Conf = StreamJob.createJob( job2);
		JobClient.runJob( job2Conf);
		
		String[] job3 = new String[]
				{
						"-mapper"   , path+"mapreduce3.py mapper" ,
					    "-reducer"  , path+"mapreduce3.py reducer" ,
					    "-input"    , "/sort/part-00000" ,
					    "-output"   , "/sum"
				};
						        
		JobConf job3Conf = StreamJob.createJob( job3);
		JobClient.runJob( job3Conf);
		
		String[] job4 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=1" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1nr" ,
						"-mapper"   , path+"mapreduce4.py mapper" ,
					    "-reducer"  , path+"mapreduce4.py reducer" ,
					    "-input"    , "/sum/part-00000" ,
					    "-output"   , "/filter"
				};
						        
		JobConf job4Conf = StreamJob.createJob( job4);
		JobClient.runJob( job4Conf);
		
		String[] job5 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2" ,
						"-mapper"   , path+"mapreduce5.py mapper" ,
					    "-reducer"  , path+"mapreduce5.py reducer" ,
					    "-input"    , "/sort/part-00000" ,
					    "-output"   , "/feature"
				};
						        
		JobConf job5Conf = StreamJob.createJob( job5);
		JobClient.runJob( job5Conf);
		return 0;
	}
    
    public static void main( String[] args) throws Exception
    {
        // ToolRunner handles generic command line options  
    	int res = ToolRunner.run( new Configuration(), new Chain(), args);
        System.exit( res);
    }//end main

}//end TestChain