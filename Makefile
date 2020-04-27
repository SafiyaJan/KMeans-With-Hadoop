2d_kmeans: 	points.java
			javac -classpath $$HADOOP_HOME/hadoop-0.20.2-core.jar -d 2d_kmeans points.java
			jar -cvf points.jar -C 2d_kmeans/ .
			hadoop dfs -rmr /user/hadoop/pointkmeans/output

dna_kmeans: dna.java
			javac -classpath $$HADOOP_HOME/hadoop-0.20.2-core.jar -d dna_kmeans dna.java
			jar -cvf dna.jar -C dna_kmeans/ .
			hadoop dfs -rmr /user/hadoop/dnakmeans/output
clean: 
		rm -rf points.jar
		rm -rf dna.jar