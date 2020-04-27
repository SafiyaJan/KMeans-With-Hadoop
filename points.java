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

public class points
{

	public static String INIT_CENTROID = "/init_centroids.csv";

	
	//Class that defines a Coord that defines and point and centroid
	public static class Coord implements WritableComparable
	{
		public double x;
		public double y;

		//Constructor
		public Coord(double x, double y)
		{
			this.x = x;
			this.y = y;
		}

		//Constructor
		public Coord()
		{
			this(0.0d,0.0d);
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeDouble(x);
			out.writeDouble(y);
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			x = in.readDouble();
			y = in.readDouble();
		}


		//Euclid distance from origin
		public double distance(Coord point) 
		{
    		return (double) Math.sqrt(Math.pow(point.x, 2) + Math.pow(point.y, 2));
		}

		
		@Override
		public int compareTo(Object o)
		{
			Coord point = (Coord)o;

			if (distance(point) < distance(this)) 
				return 1;

			if (distance(point) > distance(this)) 
				return -1;

			return 0;

		}

		public String toString()
		{
			return Double.toString(x) + "," +  Double.toString(y) + ",";
		}

		public boolean equals(Object o) 
		{
			if (!(o instanceof Coord))
				return false;

			Coord other = (Coord)o;
			return this.x == other.x && this.y == other.y;
       		
		}

		public int hashCode() 
		{
	    	return (int)(this.x + this.y);
 		}

	}

	
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Coord, Coord>
	{

		public static ArrayList<Coord> map_centroids = new ArrayList<Coord>();


		/* euclid_dist - Finds the euclid distance between two points */
		public double euclid_dist(Coord p1, Coord p2)
		{
			if (p1 == null || p2 == null)
				throw new NullPointerException();

			double x_square_diff = (p1.x - p2.x) * (p1.x - p2.x);
			double y_square_diff = (p1.y - p2.y) * (p1.y - p2.y);

			return Math.sqrt(x_square_diff + y_square_diff);
		}


		/* nearest_centroids - Finds the nearest centroid to the given point */
		public Coord nearest_centroid(Coord point)
		{
			
			if (point == null)
				throw new NullPointerException();

			System.out.println("\n");
			System.out.println("SIZE HERE - " + map_centroids.size() );
			System.out.println("\n");

			// Assume the first centroid is the closest 
			Coord min = map_centroids.get(0);

			//Find euclid dist between point and first centroid
			double dist = euclid_dist(point, map_centroids.get(0));

			//Go through list and check for closest centroid
			for (int i = 1; i < map_centroids.size(); i++)
			{
				double temp = euclid_dist(point, map_centroids.get(i));

				if(temp < dist)
				{
					dist = temp;
					min = map_centroids.get(i);
				}

			}

			//return closest centroid
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

				//Opening file for reading
				Path path_input = new Path(temp);
				FileSystem fs_input = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader( new InputStreamReader(fs_input.open(path_input)));
		
				String line;
				line = br.readLine();
				
				//Storing centroids in map_centroids
				while(line!=null)
				{
					String[] token = line.split(",");
					Coord coord = new Coord();
					coord.x = Double.parseDouble(token[0]);
					coord.y = Double.parseDouble(token[1]);
					System.out.println("\n");
					System.out.println(coord.toString());
					System.out.println("\n");
					map_centroids.add(coord);
					line = br.readLine();

				}

				System.out.println("\nNOW PRINTING MAP CENTROIDS\n");

				for(int i = 0; i < map_centroids.size(); i++ )
		        {
		        	
					System.out.println("\n");
		        	System.out.println(map_centroids.get(i).toString());
		        	System.out.println("\n");
		        	
		        }

		        br.close();	

			}

			catch(Exception e)
			{
				e.printStackTrace();
			}


		}

		/* map - emits centroid,point */
		public void map(LongWritable key, Text value, OutputCollector<Coord,Coord> output,
			Reporter report) throws IOException
		{

			Coord point = new Coord();
		
			//Get the point from value
			String[] line = value.toString().split(",");
			point.x = Double.parseDouble(line[0]);
			point.y = Double.parseDouble(line[1]);

			//Find the closest centroid to point and emit centroid,point
			output.collect(nearest_centroid(point),point);

		}

	}

	public static class Reduce extends MapReduceBase implements 
	Reducer<Coord, Coord, Text, LongWritable>
	{
		
		/* reduce - calculates the new centroids and emits (<old_cent,new_cent>,count) */
		public void reduce(Coord key, Iterator<Coord> values, 
			OutputCollector<Text, LongWritable> output, Reporter report) throws IOException
		{
			double x_count = 0;
			double y_count = 0;
			int count = 0;

			// Get all the points and sum the x and y coords
			while (values.hasNext())
			{
				Coord next = values.next();
				x_count += next.x;
				y_count += next.y;
				count++;
			}

			//Calculate the new centroid
			Coord centroid = new Coord(x_count/(double)count,y_count/(double)count);
			Text output_key = new Text();
			
			//Output old,new centroids with the number of points assigned to new centroid
			output_key.set(key.toString() + centroid.toString());
			output.collect(output_key, new LongWritable(count));

		}
	}

	/* init_centroids - select initial centroids from input data file and store initial centroids
	   in output file for mapper to read in config function */
	public static void init_centroids(String input, String output, int num_pts, int clusters)
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
               		//Split the line and get each coord
               		String[] point = line.split(",");
              
               		Coord coord = new Coord(Double.parseDouble(point[0]),
               			Double.parseDouble(point[1]));
               		                             		
               		//write centroid to file
               		bw.write(coord.toString());
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

	
	/* euclid_dist - Finds the euclid distance between two points */
	public static double euclid_dist(Coord p1, Coord p2)
	{
			if (p1 == null || p2 == null)
				throw new NullPointerException();

			double x_square_diff = (p1.x - p2.x) * (p1.x - p2.x);
			double y_square_diff = (p1.y - p2.y) * (p1.y - p2.y);

			return Math.sqrt(x_square_diff + y_square_diff);
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

		if (args.length == 5)
		{
			num_itr = Integer.parseInt(args[4]);
		}
		
	
		System.out.println("Starting clustering now... ");

		long start_time = System.currentTimeMillis();

		//Getting file name
		Path in_dir = new Path(input_dir);
		FileSystem fs_input = FileSystem.get(in_dir.toUri(),new Configuration());
		FileStatus[] files = fs_input.listStatus(in_dir);

		//Creating directory to store init centroid
		Path temp_dir = in_dir.getParent();
		FileSystem fs_temp = FileSystem.get(temp_dir.toUri(),new Configuration());
		fs_temp.mkdirs(new Path(temp_dir.toString() + "/2dtemp"));

		//Input file for selecting centroids
		String input = files[0].getPath().toString();
		//Output file for storing centroids
		String output = temp_dir.toString() + "/2dtemp" + INIT_CENTROID;

		//Selecting initial centroids
		init_centroids(input, output, num_pts, num_clusters);
		
		int k = 0;
		
		while(cur_itr < num_itr && k < num_clusters)
		{	
			
			//Creating new job and configuring it 
			JobConf conf = new JobConf(points.class);
			conf.setJobName("2D Kmeans!");

			conf.set("centroid_file", output);

			conf.addResource(new Path("/user/hadoop/conf/core-site.xml"));

			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			
			conf.setMapOutputKeyClass(Coord.class);
			conf.setMapOutputValueClass(Coord.class);
		
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(LongWritable.class);

			conf.set("mapred.tasktracker.map.tasks.maximum", "1");
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			 
			//Run job
			JobClient.runJob(conf);

			
			//Get a list of output files in output dir
			Path out_dir = new Path(output_dir);
			FileSystem out_input = FileSystem.get(out_dir.toUri(),new Configuration());
			FileStatus[] out_files = out_input.listStatus(out_dir);
			
			/*Opening file that saves the old centroids
			  We are going to write the newly calculated centroids here, so the 
			  next iteration can read these centroids */
			Path new_file = new Path(output);
			FileSystem fs1 = FileSystem.get(conf);
			FSDataOutputStream out = fs1.create(new_file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));


	        
	        k = 0;

	        /* Reading all output files that contain new and old centroids, 
	           so that we can check for convergance */

	        for(int i = 0; i < out_files.length; i++)
	        {
		        
	        	//Reading each file and storing new centroid in temp dir
	        	if(!out_files[i].isDir())
	        	{
			        
	        		//Reader for each file
	        		Path old_file = out_files[i].getPath();
					FileSystem fs = FileSystem.get(old_file.toUri(), conf);
					BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(old_file)));

					
					//Read each line
					String line;
					line = br.readLine();	

			        while(line!=null)
			        {
			  
			        	//Read the old and new centroid
			        	String[] temp = line.toString().split(",");
			        	Coord old_cent = new Coord(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
			        	Coord new_cent = new Coord(Double.parseDouble(temp[2]),Double.parseDouble(temp[3]));
			        	int count = Integer.parseInt(temp[4].trim());

			        	/* Write the new centroids to temp directory 
			        	  (this will be the  centroids for the next itr) */
			        	bw.write(new_cent.toString());
			        	bw.write(Integer.toString(count));
		               	bw.newLine();

			        	/* Check the difference between old and new centroids */
			        	double dist = euclid_dist(old_cent, new_cent);
			        	if (dist < 0.1)
			        		k++;
			        	line = br.readLine();
			        }

			        br.close();
			    }
	   		}

	        cur_itr++;

	        //Closing buffered reader and writee
	        
	        bw.close();

	        if (k == num_clusters && cur_itr == num_itr)
	        {
	        	break;
	        }
	        	
	        	

	        //Deleting output directory for next iteration
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
			bw_out.write(",");
			bw_out.write(temp[2]);
			bw_out.newLine();
			line = br_in.readLine();
		}

		/* Clean up */
		br_in.close();
		bw_out.close();

		/* Deleting other part files */
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
		System.out.println("Points Clustering Total Time Taken - " + total_time);
	


	}

}

