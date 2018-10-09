package au.edu.rmit.bdp.ClusteringReg;

import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.crypto.Data;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class App extends Configured implements Tool
{
    private static final Log LOG = LogFactory.getLog(au.edu.rmit.bdp.ClusteringReg.App.class);

    public static void main( String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new au.edu.rmit.bdp.ClusteringReg.App(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        int iteration = 1;
        Configuration conf = getConf();
        conf.set("num.iteration", iteration + "");

        //5 arguments: input, output, number of K, column 1, column 2
        Path pointDataPath = new Path(args[1] + "/data.seq");
        Path centroidDataPath = new Path(args[1] + "/centroid.seq");
        conf.set("centroid.path", centroidDataPath.toString());
        Path outputDir = new Path(args[1] + "/clustering/depth_1");

        //Job job = new Job(conf, "Clustering"); *deprecated
        Job job = Job.getInstance(conf, "KMeans App Without IMC");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setJarByClass(KMeansMapper.class);


        //Check the data if it's available or not
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        if (fs.exists(centroidDataPath)) {
            fs.delete(centroidDataPath, true);
        }

        if (fs.exists(pointDataPath)) {
            fs.delete(pointDataPath, true);
        }

        //Lists for columns 1 and 2 data points
        List<Double> col1 = new ArrayList<>();
        List<Double> col2 = new ArrayList<>();

        //Generate the points previously so the dataPoints is available for input file
        generatePoints(args, conf, pointDataPath, col1, col2);

        FileInputFormat.setInputPaths(job, pointDataPath);

        //Generate centroids based on the maximum points available (randomised)
        generateCentroids(args, conf, centroidDataPath, col1, col2);

        int numReduceTasks = (args.length == 6) ? Integer.parseInt(args[5]) : 1;

        job.setNumReduceTasks(numReduceTasks);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(DataPoint.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KMeansReducer.Counter2.CONVERGED).getValue();
        iteration++;
        while (counter > 0 && iteration < 101){
            conf = new Configuration();
            conf.set("centroid.path", centroidDataPath.toString());
            conf.set("num.iteration", iteration + "");
            job = Job.getInstance(conf, "KMeans App " + iteration);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setJarByClass(KMeansMapper.class);

            pointDataPath = new Path(args[1] + "/clustering/depth_" + (iteration - 1) + "/");
            outputDir = new Path(args[1] + "/clustering/depth_" + iteration);

            FileInputFormat.addInputPath(job, pointDataPath);
            if (fs.exists(outputDir))
                fs.delete(outputDir, true);

            FileOutputFormat.setOutputPath(job, outputDir);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(DataPoint.class);
            job.setNumReduceTasks(numReduceTasks);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(KMeansReducer.Counter2.CONVERGED).getValue();
            System.out.println(counter);
        }
        LOG.info("JOB COMPLETED. Final output at: " + args[1] + "/clustering/depth_" + (iteration - 1) + "/");
        return 0;
    }


    public static void generatePoints(String[] args, Configuration conf, Path out, List<Double> col1, List<Double> col2) throws IOException {
        try (Reader in = new FileReader(args[0])){
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);

            // non deprecated func uses option class
            SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
            SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
            SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(DataPoint.class);
            try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue)) {

                int labels = 0;
                //Get the column 1 and 2 to list
                for (CSVRecord record : records) {
                    if (labels == 0) {
                        //do something for the labels
                        labels++;
                    } else {
                        String columnOne = record.get(Integer.valueOf(args[3]));
                        String columnTwo = record.get(Integer.valueOf(args[4]));
                        double point1 = Double.parseDouble(columnOne);
                        double point2 = Double.parseDouble(columnTwo);
                        dataWriter.append(new Centroid(new DataPoint(0, 0)), new DataPoint(point1, point2));
                        col1.add(point1);
                        col2.add(point2);
                    }
                }
            }
        }
    }

    //Taking the amount or K from user input from argument 2 and create random clusters
    public static void generateCentroids(String[] args, Configuration conf, Path out, List<Double> col1, List<Double> col2) throws
            IOException, InterruptedException{
        final IntWritable value = new IntWritable(0);
        SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
        SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
        SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(IntWritable.class);
        try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue)) {
            for (int i = 0; i < Integer.valueOf(args[2]); i++) {
                Random r = new Random();
                centerWriter.append(new Centroid(new DataPoint(r.nextInt(Collections.max(col1).intValue()), r.nextInt(Collections.max(col2).intValue()))), value);
            }
        }

    }
}
