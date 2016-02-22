import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class KMeans extends Configured implements Tool{
	public static String path = "/Users/Galle/Large-Scale-Assigment1/";
	public static String input_path = "/feature/part-00000";
	public static String output_path = "";
	public static String center_path = "";
	public static String old_center_path = "";
	public static final int MAX_ITERATION = 100;
	public static final double MIN_THRESHOLD = 0.1;

	
	public static void main(String[] args) throws Exception {
		boolean isdone = false;
		int iteration = 0;
		center_path = "/center-" + iteration + "/part-00000";
		output_path = "/center-"+ (iteration+1);
		while(iteration < MAX_ITERATION && !isdone) {
			int res = ToolRunner.run( new Configuration(), new KMeans(), args);
			
			// Old center path input from current step
			old_center_path = "/center-" + iteration + "/part-00000";
			
			++iteration;
			
			// Center path output from current step
			center_path = "/center-" + iteration + "/part-00000";
			
			// Output center path for next step
			output_path = "/center-" + (iteration+1);
			
			// Read file from current center path
			Path ofile = new Path(center_path);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			ArrayList<ArrayList<Double>> centers_next = new ArrayList<ArrayList<Double>>();
		
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t");
				double c = Double.parseDouble(sp[0]);
				
				ArrayList<Double> point = new ArrayList<Double>();
				point.add(c);
				centers_next.add(point);
				line = br.readLine();
			}
			br.close();
			
			// Read file from previous center path
			Path prevfile = new Path(old_center_path);
			FileSystem fs1 = FileSystem.get(new Configuration());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(
					fs1.open(prevfile)));
			ArrayList<ArrayList<Double>> centers_prev = new ArrayList<ArrayList<Double>>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split("\t");
				double c = Double.parseDouble(sp1[0]);
				ArrayList<Double> point = new ArrayList<Double>();
				point.add(c);
				centers_prev.add(point);
				l = br1.readLine();
			}
			br1.close();
			
			Collections.sort(centers_next, new Comparator<ArrayList<Double>>() {
				@Override
				public int compare(ArrayList<Double> item1,
						ArrayList<Double> item2) {

					for(int i = 0;i < item1.size();i++) {
						if(item1.get(i)<item2.get(i)) return -1;
					}
					return 0;
				}
		    });
			
			Collections.sort(centers_prev, new Comparator<ArrayList<Double>>() {
				@Override
				public int compare(ArrayList<Double> item1,
						ArrayList<Double> item2) {

					for(int i = 0;i < item1.size();i++) {
						if(item1.get(i)<item2.get(i)) return -1;
					}
					return 0;
				}
		    });
			
			Iterator<ArrayList<Double>> it = centers_prev.iterator();
			for (ArrayList<Double> d : centers_next) {
				ArrayList<Double> temp = it.next();
				double diff = 0;
				for(int i=0;i < temp.size();i++) {
					double feature1 = d.get(i);
					double feature2 = temp.get(i);
					diff += (feature1-feature2)*(feature1-feature2);
				}
				if (Math.sqrt(diff) > MIN_THRESHOLD) {
					isdone = false;
					break;
				} else {
					isdone = true;
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		String[] job = new String[]
				{
						"-jobconf"  , "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" ,
						"-jobconf"  , "stream.num.map.output.key.fields=1" ,
						"-jobconf"  , "mapred.text.key.comparator.options=-k1,1" ,
						"-mapper"   , path+"mapreduce6.py mapper "+center_path ,
					    "-reducer"  , path+"mapreduce6.py reducer "+center_path,
					    "-input"    , input_path ,
					    "-output"   , output_path
				};
						        
		JobConf jobConf = StreamJob.createJob( job);
		JobClient.runJob( jobConf);
		return 0;
	}
}