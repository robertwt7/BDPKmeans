package au.edu.rmit.bdp.ClusteringIMC;


import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Commons-csv
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new App(), args));
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        int iteration = 1;
        Configuration conf = getConf();
        conf.set("num.iteration", iteration + "");

        //5 arguments: input, output, number of K, column 1, column 2
        Path pointDataPath = new Path(args[0] + "/data.seq");
        Path centroidDataPath = new Path(args[0] + "/centroid.seq");
        conf.set("centroid.path", centroidDataPath.toString());
        Path outputDir = new Path(args[1] + "clustering/depth_1");

        //Model 1, with inMapper Combiner
        //Job job = new Job(conf, "Clustering"); *deprecated
        Job job = Job.getInstance(conf, "KMeans App");

        job.setMapperClass(KmeansMapping.class);
        job.setReducerClass(KmeansReducing.class);
        job.setJarByClass(App.class);

        FileInputFormat.setInputPaths(job, pointDataPath);
        FileSystem fs = FileSystem.get(conf);
        FileOutputFormat.setOutputPath(job, outputDir);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        if (fs.exists(centroidDataPath)) {
            fs.delete(centroidDataPath, true);
        }

        if (fs.exists(pointDataPath)) {
            fs.delete(pointDataPath, true);
        }

        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }

    //5 arguments: input, output, number of K, column 1, column 2
    //Read csv from 1st arguments
    public static void generatePoints(String[] args, Configuration conf, Path out, FileSystem fs) throws IOException{
        try (Reader in = new FileReader(args[0])){
            List<Integer> col1 = new ArrayList<Integer>();
            List<Integer> col2 = new ArrayList<Integer>();

            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

            // non deprecated func uses option class
            SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
            SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
            SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(DataPoint.class);
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue);

            //Get the column 1 and 2 to list
            for (CSVRecord record : records){
                String columnOne = record.get(args[3]);
                String columnTwo = record.get((args[4]));
                int point1 = Integer.valueOf(columnOne);
                int point2 = Integer.valueOf(columnTwo);
                dataWriter.append(new Centroid(new DataPoint(0,0)), new DataPoint(point1,point2));
                col1.add(point1);
                col2.add(point2);
            }
        }
    }

    //Taking the amount or K from user input from argument 2 and create random clusters
    public static void generateCentroids(String[] args, Configuration conf, Path out, List<Integer> col1, List<Integer> col2) throws IOException, InterruptedException{
        SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
        SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
        SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(IntWritable.class);
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue);
        for (int i = 0; i < Integer.valueOf(args[2]); i++){
            Random r = new Random();
            centerWriter.append(new Centroid(new DataPoint(r.nextInt(Collections.max(col1)), r.nextInt(Collections.max(col2)))), opValue);
        }

    }
}
