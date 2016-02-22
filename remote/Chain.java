import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;

public class Chain extends Configured implements Tool
{
    public static String code_path = "/home/hadoop/clustering/mapreduce";
    public static String output_path = "/user/hadoop/project_dekd";
    
	@Override
	public int run(String[] arg0) throws Exception {
		String[] job1 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2" ,
						"-files"	, "/home/hadoop/clustering/mapreduce/mapreduce1.py" ,
						"-mapper"   , code_path+"mapreduce1.py mapper" ,
				        "-reducer"  , code_path+"mapreduce1.py reducer" ,
				        "-input"    , "DekD/*/*/*.txt" ,
				        "-output"   , output_path+"/output" ,
				};
				        
		JobConf job1Conf = StreamJob.createJob( job1);
		JobClient.runJob( job1Conf);
		
		String[] job2 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2nr" ,
						"-file"	, "/home/hadoop/clustering/mapreduce/mapreduce2.py" ,
						"-mapper"   , code_path+"mapreduce2.py mapper" ,
			            "-reducer"  , code_path+"mapreduce2.py reducer" ,
			            "-input"    , output_path+"/output/part-00000" ,
			            "-output"   , output_path+"/sort" ,
				};
				        
		JobConf job2Conf = StreamJob.createJob( job2);
		JobClient.runJob( job2Conf);
		
		String[] job3 = new String[]
				{
						"-file"	    , "/home/hadoop/clustering/mapreduce/mapreduce3.py" ,
						"-mapper"   , code_path+"mapreduce3.py mapper" ,
					    "-reducer"  , code_path+"mapreduce3.py reducer" ,
					    "-input"    , output_path+"/sort/part-00000" ,
					    "-output"   , output_path+"/sum"
				};
						        
		JobConf job3Conf = StreamJob.createJob( job3);
		JobClient.runJob( job3Conf);
		
		String[] job4 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=1" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1nr" ,
						"-file"	    , "/home/hadoop/clustering/mapreduce/mapreduce4.py" ,
						"-mapper"   , code_path+"mapreduce4.py mapper" ,
					    "-reducer"  , code_path+"mapreduce4.py reducer" ,
					    "-input"    , output_path+"/sum/part-00000" ,
					    "-output"   , output_path+"/filter"
				};
						        
		JobConf job4Conf = StreamJob.createJob( job4);
		JobClient.runJob( job4Conf);
		
		String[] job5 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2" ,
						"-file"		, "/home/hadoop/clustering/mapreduce/mapreduce5.py,hdfs://172.16.2.1:9000/user/hadoop/project_dekd/filter/part-00000" ,
						"-mapper"   , code_path+"mapreduce5.py mapper" ,
					    "-reducer"  , code_path+"mapreduce5.py reducer" ,
					    "-input"    , output_path+"/sort/part-00000" ,
					    "-output"   , output_path+"/feature"
				};
						        
		JobConf job5Conf = StreamJob.createJob( job5);
		JobClient.runJob( job5Conf);

		String[] job6 = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=2" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1 -k2,2" ,
						"-file"		, "/home/hadoop/clustering/mapreduce/mapreduce6.py" ,
						"-mapper"   , code_path+"mapreduce6.py mapper" ,
					    "-reducer"  , code_path+"mapreduce6.py reducer" ,
					    "-input"    , output_path+"/feature/part-00000" ,
					    "-output"   , output_path+"/result"
				};

		JobConf job6Conf = StreamJob.createJob( job6);
		JobClient.runJob( job6Conf);
		return 0;
	}
    
    public static void main( String[] args) throws Exception
    {
        // ToolRunner handles generic command line options  
    	int res = ToolRunner.run( new Configuration(), new Chain(), args);
        System.exit( res);
    }//end main

}//end TestChain