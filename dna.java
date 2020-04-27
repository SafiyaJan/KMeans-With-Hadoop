import java.util.*;
import java.io.*;
import java.lang.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class dna
{
	public static String INIT_CENTROID = "/init_centroids.csv";

	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, Text>
	{

		public static ArrayList<Text> map_centroids = new ArrayList<Text>();
		public static int length = 0;


		/* dist_dna - Calculate distance between towo strands */
		public int dist_dna(Text p1, Text p2, int length)
		{
			if (p1 == null || p2 == null)
				throw new NullPointerException();
			

			int i = 0;
			int diff_count = 0;

			String [] strand1 = p1.toString().split("");
			String [] strand2 = p2.toString().split("");


			for (i = 0; i < length; i ++)
			{
				if (!strand1[i].equals(strand2[i]))
		            diff_count++;
			} 

			return diff_count;
		}


		/* nearest_centroids - Finds the nearest centroid to the given point */
		public Text nearest_centroid(Text strand, int length)
		{
			if(strand == null)
				throw new NullPointerException();

			// Assume the first centroid is the closest 
			Text min = map_centroids.get(0);

			//Find euclid dist between point and first centroid
			int dist = dist_dna(strand, map_centroids.get(0),length);



			//Go through list and check for closest centroid
			for(int i = 1; i < map_centroids.size(); i++)
			{
				int temp = dist_dna(strand,map_centroids.get(i),length);

				System.out.println("The difference is - " + temp);

				if (temp < dist)
				{
					dist = temp;
					min = map_centroids.get(i);
				}
			}

			//return closest centroid
			System.out.println("NEAREST IS - " + min.toString());
			return min;
		}

		/* config - reads the centroids from file and stores in array of centroids */
		@Override
		public void configure(JobConf job) 
		{

			try
			{
				//Get the name of the file
				String temp = job.get("centroid_file");
				length = Integer.parseInt(job.get("length"));

				//Opening file for reading
				Path path_input = new Path(temp);
				FileSystem fs_input = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader( new InputStreamReader(fs_input.open(path_input)));
		
				String line;
				line = br.readLine();
				
				//Storing centroids in map_centroids
				while(line!=null)
				{
					Text strand = new Text(line.replaceAll(",",""));

					System.out.println("INIT Centroid is - " + strand.toString());

					map_centroids.add(strand);
					line = br.readLine();
				}

		        br.close();	

			}

			catch(Exception e)
			{
				e.printStackTrace();
			}


		}

		/* map - emits centroid,point */
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output,
			Reporter report) throws IOException
		{
			String line = value.toString().replace(",","");
			Text strand = new Text(line);
			
			System.out.println("MAP VALUE IS " + strand.toString());

			output.collect(nearest_centroid(strand,length),strand);

		}

	}

	public static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, LongWritable>
	{
		
		//Class for keeping track of base counts
		public static class Base_Count
		{
			public int A;
			public int C;
			public int T;
			public int G;

			public Base_Count()
			{
				this.A = 0;
				this.C = 0;
				this.T = 0;
				this.G = 0;
			}

		}

		public static int length = 0;

		@Override
		public void configure(JobConf job) 
		{
			
			//get length of dna stranc
			length = Integer.parseInt(job.get("length"));

		}
		

		/* reduce - calculates the new centroids and emits (<old_cent,new_cent>,count) */
		public void reduce(Text key, Iterator<Text> values, 
			OutputCollector<Text, LongWritable> output, Reporter report) throws IOException
		{
			
			int num_pts = 0;

			ArrayList<Base_Count> count = new ArrayList<Base_Count>();

			System.out.println("Reduce Length - " + length);
			
			for(int i = 0; i < length; i++)
				count.add(new Base_Count());

			//Go through each value for one centroid and calculate the base counts for each index
			while(values.hasNext())
			{
				Text string = new Text(values.next());
				
				System.out.println("value - " + string.toString());

				char[] temp = string.toString().toCharArray();

				//Increment count for base
				for(int i = 0; i < length; i++)
				{
					if(temp[i] == 'A')
						count.get(i).A++;
					
					if(temp[i] == 'G')
						count.get(i).G++;
					
					if(temp[i] == 'C')
						count.get(i).C++;
					
					if(temp[i] == 'T')
						count.get(i).T++;

				}

				num_pts++;

			}

			String[] temp_cent = new String[length];

			// Select max occuring base for each index 
			for(int i = 0; i < length; i++)
			{
				int max_count = 0;
			

				if (count.get(i).A > max_count)
				{	
					
					max_count = count.get(i).A;
					temp_cent[i] = "A";
				}

				if (count.get(i).C > max_count)
				{	
					
					max_count = count.get(i).C;
					temp_cent[i] = "C";
				}

				if (count.get(i).T > max_count)
				{	
					
					max_count = count.get(i).T;
					temp_cent[i] = "T";
				}

				if (count.get(i).G > max_count)
				{	
					
					max_count = count.get(i).G;
					temp_cent[i] = "G";
				}

			}

			
			String new_cent = "";
			for (String s : temp_cent) {
  				new_cent += s;
			}

			// Output old centroid, new centroid and count
			Text out_key = new Text(key.toString() + "," + new_cent + ",");

			output.collect(out_key, new LongWritable(num_pts));

		
		}


	}

	/* init_centroids - select initial centroids from input data file and store initial centroids
   	   in output file for mapper to read in config function */
	public static void init_centroids(String input, String output, int num_pts, int clusters, int length)
	{
		try
		{
			// Reader for reading input data file
			Path path_input = new Path(input);
			FileSystem fs_input = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader( new InputStreamReader(fs_input.open(path_input)));
			
			//Writer for writing to output file
			Path path_output = new Path(output);
			FileSystem fs_output = FileSystem.get(new Configuration());
			FSDataOutputStream out = fs_output.create(path_output);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

			String line;
        	
        	line = br.readLine();
        	
        	int index = 0;
        	int count = 0;

        	// Reading the input file
        	while (line != null)
        	{
               	//Write the centroids at this line index into the output file
               	if (count == index)
               	{
               
              		//Writing centroid
               		bw.write(line);
               		bw.newLine();

               		//next line index to read from
               		index += num_pts/clusters;
               	}

                line = br.readLine();
                count++;

            }

            // Close reader and writer
            bw.close();
        	br.close();

        }

        catch(Exception e)
        {
        	e.printStackTrace();
        }


	}

	/* dist_dna - Calculate distance between towo strands */
	public static int dist_dna(Text p1, Text p2, int length)
		{
			if (p1 == null || p2 == null)
				throw new NullPointerException();
			

			int i = 0;
			int diff_count = 0;

			String [] strand1 = p1.toString().split("");
			String [] strand2 = p2.toString().split("");


			for (i = 0; i < length; i ++)
			{
				if (!strand1[i].equals(strand2[i]))
		            diff_count++;
			} 

			return diff_count;
		}



	public static void main (String args[]) throws Exception
	{
		//Get all command line inputs 
		String input_dir = args[0];
		String output_dir = args[1];
		
		int num_pts = Integer.parseInt(args[2]);
		int num_clusters = Integer.parseInt(args[3]);

		int num_itr = 1;

		int cur_itr = 0;

		int length = 0;

		if (args.length == 6)
		{
			num_itr = Integer.parseInt(args[4]);
			length = Integer.parseInt(args[5]);
		}

		else 
		{
			length = Integer.parseInt(args[4]);
		}

		long start_time = System.currentTimeMillis();

		//Getting file name
		Path in_dir = new Path(input_dir);
		FileSystem fs_input = FileSystem.get(in_dir.toUri(),new Configuration());
		FileStatus[] files = fs_input.listStatus(in_dir);

		//Creating directory to store init centroid
		Path temp_dir = in_dir.getParent();
		FileSystem fs_temp = FileSystem.get(temp_dir.toUri(),new Configuration());
		fs_temp.mkdirs(new Path(temp_dir.toString() + "/dnatemp"));

		//Input file for selecting centroids
		String input = files[0].getPath().toString();
		//Output file for storing centroids
		String output = temp_dir.toString() + "/dnatemp" + INIT_CENTROID;

		//Selecting initial centroids
		init_centroids(input, output, num_pts, num_clusters, length);

		int k = 0;

		System.out.println("Starting clustering now... ");

		while(cur_itr<num_itr && k < num_clusters)
		{
			//Creating new job and configuring it 
			JobConf conf = new JobConf(dna.class);
			conf.setJobName("DNA Kmeans!");

			conf.set("centroid_file", output);
			conf.set("length", Integer.toString(length));

			conf.addResource(new Path("/user/hadoop/conf/core-site.xml"));

			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
		
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));

			JobClient.runJob(conf);

			//Get list of output files in output directory
			Path out_dir = new Path(output_dir);
			FileSystem out_input = FileSystem.get(out_dir.toUri(),new Configuration());
			FileStatus[] out_files = out_input.listStatus(out_dir);

			//Open file that saves old centroids
			Path new_file = new Path(output);
			FileSystem fs1 = FileSystem.get(conf);
			FSDataOutputStream out = fs1.create(new_file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

			k = 0;

			for(int i = 0; i < out_files.length; i++)
			{
				//Reading each file and storing new centroid in temp dir
				if (!out_files[i].isDir())
				{
					Path old_file = out_files[i].getPath();
					FileSystem fs = FileSystem.get(old_file.toUri(), conf);
					BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(old_file)));

					String line;
					line = br.readLine();

					//read each line
					while(line!=null)
					{
						String[] temp = line.toString().split(",");
						String old_cent = temp[0];
						String new_cent = temp[1];
						int count = Integer.parseInt(temp[2].trim());
						
						//write the latest centroid to temp dir
						bw.write(new_cent);
						bw.write(",");
						bw.write(Integer.toString(count));
						bw.newLine();

						/* Check the difference between old and new centroids */
						int dist = dist_dna(new Text(old_cent), new Text(new_cent), length);
						
						if (dist <= 0.2*length)
							k++;
						
						line = br.readLine();

					}

					br.close();
				}
			}

			bw.close();

			cur_itr++;

			//If all centroids have stopped changing, stop iterating
			if (k == num_clusters)
				break;

			//Else delete output directory and iterate once more
			Path output_delete = new Path(output_dir);
			FileSystem hdfs = FileSystem.get(output_delete.toUri(),conf);
	        if (hdfs.exists(output_delete)) {
   				hdfs.delete(output_delete, true);
			}

		}


		/*Writing final centroids to output file*/
		Path output_file = new Path(output_dir + "/part-finalcentroids");
		FileSystem out_fs = FileSystem.get(new Configuration());
		FSDataOutputStream out_stream = out_fs.create(output_file);
		BufferedWriter bw_out = new BufferedWriter(new OutputStreamWriter(out_stream));

		/* Read saved centroids from temporary directory */
		Path temp_file = new Path(output);
		FileSystem fs_in = FileSystem.get(temp_file.toUri(), new Configuration());
		BufferedReader br_in = new BufferedReader( new InputStreamReader(fs_in.open(temp_file)));

		String line;	
	    line = br_in.readLine();
	    
	    /* Go through file in temporary directory, read each centroid 
	    and write the centroid to output file */
		while(line!=null)
		{
			String[] temp = line.toString().split(",");
		
			bw_out.write(temp[0]);
			bw_out.write(",");
			bw_out.write(temp[1]);
			bw_out.newLine();
			line = br_in.readLine();
		}

		/* Clean up */
		br_in.close();
		bw_out.close();

		//Delete other path files in output directories
		Path temp = new Path(output_dir);
		FileSystem out_temp = FileSystem.get(temp.toUri(),new Configuration());
		FileStatus[] temp_files = out_temp.listStatus(temp);

		for(int i = 0; i < temp_files.length; i++)
		{
			if (!temp_files[i].isDir() && temp_files[i].getPath().toString().contains("part-0"))
			{
				FileSystem delete_file = FileSystem.get(temp_files[i].getPath().toUri(),new Configuration());
				if (delete_file.exists(temp_files[i].getPath()))
					delete_file.delete(temp_files[i].getPath());

			}

		}


		long end_time = System.currentTimeMillis();

		double total_time = (end_time-start_time)/1000;

		System.out.println("Clustering completed... ");
		System.out.println("DNA Clustering Total Time Taken - " + total_time);



		
	}

}