import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import java.net.URI;

public class MRMiner{
    static int minSupp;
	static int corr;
    static int transPerBlock;
    static int newminSupp;
    static int totaltransactions = 4;
	public static void setMinSuppCorr(int m,int c,int t){
		minSupp = m;
		corr = c;
        transPerBlock = t;
        newminSupp = (minSupp-corr)*(transPerBlock/totaltransactions);
		}
    public static class firstMapper
        extends Mapper<Object, Text, Text, IntWritable>{
        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException { 
						
	
	    String[] transactions = value.toString().split("\n");
        HashMap<HashSet<Integer> , Integer> itemsets = new HashMap<HashSet<Integer>, Integer>();
        HashMap<HashSet<Integer> , Integer> freqitemsets = new HashMap<HashSet<Integer>, Integer>();
        HashMap<HashSet<Integer> , Integer> freqitemsetsk = new HashMap<HashSet<Integer>, Integer>();
        int k = 1;
     
		//Creating singletons
	    for (int q=0;q<transactions.length;q++){
		    String line = transactions[q];
		    String[] items = line.split("\\s+");
		    for(int i =0; i<items.length;i++){
				HashSet<Integer> itemset = new HashSet<Integer>();
			    Integer item = Integer.valueOf(items[i]);
                itemset.add(item);
                if (!itemsets.containsKey(itemset)){
                    itemsets.put(itemset, 1);
                   // freqitemsets.put(itemset,1);
                    freqitemsetsk.put(itemset,1);
                }
                else{
                    itemsets.replace(itemset, itemsets.get(itemset)+1);
                    //freqitemsets.replace(itemset, itemsets.get(itemset)+1);
                    freqitemsetsk.replace(itemset, itemsets.get(itemset)+1);
                }	
            }	
        }
        for ( HashSet<Integer> n : itemsets.keySet()) {
            if(itemsets.get(n) < newminSupp){
            //freqitemsets.remove(n);
            freqitemsetsk.remove(n);
            }
          } 
          System.out.println(freqitemsetsk.keySet());
          boolean terminate = false;
          //Apriori
         while(!terminate){ 
            HashMap<HashSet<Integer> , Integer> freqitemsetsknext = new HashMap<HashSet<Integer>, Integer>();
            HashSet<HashSet<Integer>> nonFreqs = new HashSet<HashSet<Integer>>();
            //create new frequent itemsets 
            for (HashSet<Integer> m : freqitemsetsk.keySet()) {
                for (HashSet<Integer> l : freqitemsetsk.keySet()) {
                    int r = l.size();
                    HashSet<Integer> mCandidate = new HashSet<Integer>(); //a copy of m to use in making a candidate
                    Iterator<Integer> itr_m = m.iterator();
                    while(itr_m.hasNext()){
                        Integer t = itr_m.next();
                        mCandidate.add(t);
                    }
                    mCandidate.addAll(l); //generating a candidate
		 //checking is the generated itemset is of the size required, is frequent and has not already been generated,
                    if(mCandidate.size()== r+1 && !freqitemsetsknext.containsKey(mCandidate) && !nonFreqs.contains(mCandidate)){
                        boolean found =true;
                        Iterator<Integer> forM = mCandidate.iterator();
                        HashSet<Integer> mCandidate1 = new HashSet<Integer>();
                        HashSet<Integer> mCandidate2 = new HashSet<Integer>();
                        while(forM.hasNext()) {
                            Integer o = forM.next();
                            mCandidate1.add(o);
                            mCandidate2.add(o);
                        }
                        Iterator<Integer> forM1 = mCandidate1.iterator();
                        while(forM1.hasNext()) {
                            Integer h = forM1.next();
                            mCandidate2.remove(h);
                            if(!freqitemsetsk.containsKey(mCandidate2)){
                                found = false;
                                break;
                            }
                            mCandidate2.add(h);
                        }
                        if(found){
                            freqitemsetsknext.put(mCandidate, 1);
                        }
                        else{
                            nonFreqs.add(mCandidate);
                        }
                    }
                }
             
            } 
            if(freqitemsetsknext.size() != 0){  // if we have new frequent itemsets
                for (HashSet<Integer> newFreq:freqitemsetsknext.keySet())
					{
						freqitemsets.put(newFreq, 0);
					}
                freqitemsetsk.clear();
                freqitemsetsk.putAll(freqitemsetsknext);
                System.out.println(freqitemsetsk.keySet());
            }
            else{ //if we haven't gotten new frequent itemsets
               terminate = true; 
            }
         }
           // Set support of all frequent candidates to 0
			for (HashSet<Integer> w :freqitemsets.keySet())
			{
				freqitemsets.replace(w, 0);
			}
			
			// finding the support of each frequent itemset
			for (int u =0;u<transactions.length;u++)
			{
				String l = transactions[u];
				for (HashSet<Integer> kp :freqitemsets.keySet())
				{
					Iterator<Integer> kp_iter = kp.iterator();
					boolean find = true;
					while (kp_iter.hasNext())
					{
						Integer anitm = kp_iter.next();
						String anitem = Integer.toString(anitm.intValue());
						if (!l.contains(anitem))
						{
							find = false;
							break;
						}
					}
					if (find)
					{
						Integer support = freqitemsets.get(kp);
						freqitemsets.replace(kp, support+1);
					}
				}
			}
			
			// The output
			for (HashSet<Integer> kl : freqitemsets.keySet())
			{
				double support = freqitemsets.get(kl);
			
				if (support >= newminSupp)
				{
					String candidatetoprint = "";
					Iterator<Integer> iter = kl.iterator();
					while (iter.hasNext())
					{
						Integer i = iter.next();
						candidatetoprint = candidatetoprint + Integer.toString(i.intValue()) + " ";
					}
					context.write(new Text(candidatetoprint.trim()), new IntWritable(1));
				}
				
			}		 	
         
    }
  }
  public static class secondMapper
  	extends Mapper<Object, Text, Text, IntWritable>{
    	
	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException{
		    
		    ArrayList<String> itemsets = new ArrayList<String>();
		    URI[] myCacheFiles = context.getCacheFiles();
		    Path forPatterns = new Path(myCacheFiles[0].getPath());
          	String patternsFileName = forPatterns.getName().toString();
		    File file = new File(patternsFileName);
		    FileReader fr = new FileReader(file);
		    BufferedReader br = new BufferedReader(fr);
		    String freqCandidate;
		    while ((freqCandidate=br.readLine())!=null){
		    	itemsets.add(freqCandidate.trim());
		    }
		  
		    String[] transactions = value.toString().split("/n");
		    int[] count = new int[itemsets.size()];
		    
		    for (int i=0; i<transactions.length; i++)
		    {
				String line = transactions[i];
				for (int j=0; j<itemsets.size();j++){
					boolean found = true;
					String[] itemset = itemsets.get(j).split("\\s+");
					for (String item : itemset){
						if (!line.contains(item.trim())){
							found = false;
							break;
						}
					}
					if (found) count[j]++;
				}
		    }
		    
		    for (int i=0; i<itemsets.size(); i++){
		    	context.write(new Text(itemsets.get(i)), new IntWritable(count[i]));		   
		    }	
	}
  }
  public static class firstReducer
       extends Reducer<Text,Text,Text,NullWritable>{

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		       context.write(key, NullWritable.get());
    }
  }

  public static class secondReducer
  	extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, 
			Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count:values){
				sum+=count.get();
			}
			if (sum >= minSupp){
				context.write(key, new IntWritable(sum));
			}			
	}	
  }

  
  public static void main(String[] args) throws Exception {
	MRMiner.setMinSuppCorr(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MR Miner 0");
    job.setJar("mr.jar");
    job.setJarByClass(MRMiner.class);
    job.setInputFormatClass(MultiLineInputFormat.class);
    job.setMapperClass(firstMapper.class);
    job.setReducerClass(firstReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(args[4]));
    NLineInputFormat.setNumLinesPerSplit(job, Integer.parseInt(args[2]));
    job.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "MR Miner 1");
    job2.addCacheFile(new Path("firstout/part-r-00000").toUri());
    job2.setJar("mr.jar");
    job2.setJarByClass(MRMiner.class);
    job2.setInputFormatClass(MultiLineInputFormat.class);
    job2.setMapperClass(secondMapper.class);
    job2.setReducerClass(secondReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2,new Path(args[3]));
    FileOutputFormat.setOutputPath(job2, new Path(args[5]));
    NLineInputFormat.setNumLinesPerSplit(job2, Integer.parseInt(args[2]));
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
  
}
