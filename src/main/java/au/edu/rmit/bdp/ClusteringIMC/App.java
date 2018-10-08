package au.edu.rmit.bdp.ClusteringIMC;


import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Commons-csv
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    private static final Log LOG = LogFactory.getLog(App.class);

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
        Path pointDataPath = new Path(args[1] + "/data.seq");
        Path centroidDataPath = new Path(args[1] + "/centroid.seq");
        conf.set("centroid.path", centroidDataPath.toString());
        Path outputDir = new Path(args[1] + "/clustering/depth_1");

        //Model 1, with inMapper Combiner
        //Job job = new Job(conf, "Clustering"); *deprecated
        Job job = Job.getInstance(conf, "KMeans App");

        job.setMapperClass(KmeansMapping.class);
        job.setReducerClass(KmeansReducing.class);
        job.setJarByClass(KmeansMapping.class);
        job.setMapOutputKeyClass(Centroid.class);
        job.setMapOutputValueClass(Text.class);

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
        List<Integer> col1 = new ArrayList<Integer>();
        List<Integer> col2 = new ArrayList<Integer>();

        //Generate the points previously so the dataPoints is available for input file
        generatePoints(args, conf, pointDataPath, col1, col2);

        FileInputFormat.setInputPaths(job, pointDataPath);

        //Generate centroids based on the maximum points available (randomised)
        generateCentroids(args, conf, centroidDataPath, col1, col2);

        job.setNumReduceTasks(2);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(DataPoint.class);

        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(KmeansReducing.Counter.CONVERGED).getValue();
        iteration++;
        while (counter > 0){
            conf = new Configuration();
            conf.set("centroid.path", centroidDataPath.toString());
            conf.set("num.iteration", iteration + "");
            job = Job.getInstance(conf, "KMeans App " + iteration);

            job.setMapperClass(KmeansMapping.class);
            job.setReducerClass(KmeansReducing.class);
            job.setJarByClass(KmeansMapping.class);

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
            job.setMapOutputKeyClass(Centroid.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(3);

            job.waitForCompletion(true);
            iteration++;
            counter = job.getCounters().findCounter(KmeansReducing.Counter.CONVERGED).getValue();
        }

        Path result = new Path(args[1] + "/clustering/depth_" + (iteration - 1) + "/");
        FileStatus[] stati = fs.listStatus(result);
        for (FileStatus status : stati) {
            if (!status.isDirectory()) {
                Path path = status.getPath();
                if (!path.getName().equals("_SUCCESS")) {
                    LOG.info("FOUND " + path.toString());
                    SequenceFile.Reader.Option opPath = SequenceFile.Reader.file(path);
                    try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, opPath)){
                        Centroid key = new Centroid();
                        DataPoint v = new DataPoint();
                        while (reader.next(key, v)){
                            LOG.info(key + " / " + v);
                        }
                    }
                }
            }

        }
        return 0;
    }

    //5 arguments: input, output, number of K, column 1, column 2
    //Read csv from 1st arguments
    public static void generatePoints(String[] args, Configuration conf, Path out, List<Integer> col1, List<Integer> col2) throws IOException{
        try (Reader in = new FileReader(args[0])){
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);

            // non deprecated func uses option class
            SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
            SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
            SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(DataPoint.class);
            SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue);

            //Get the column 1 and 2 to list
            int labels = 0;
            for (CSVRecord record : records){
                if (labels == 0)
                {
                    //do something for the labels
                    labels ++;
                } else {
                    String columnOne = record.get(Integer.valueOf(args[3]));
                    String columnTwo = record.get(Integer.valueOf(args[4]));
                    int point1 = Integer.valueOf(columnOne);
                    int point2 = Integer.valueOf(columnTwo);
                    dataWriter.append(new Centroid(new DataPoint(0,0)), new DataPoint(point1,point2));
                    col1.add(point1);
                    col2.add(point2);
                    labels++;
                }
            }
            IOUtils.closeStream(dataWriter);
        }
    }

    static void printLongerTrace(Throwable t){
        for(StackTraceElement e: t.getStackTrace()){
            LOG.error(e);
            System.out.println(e);
        }
    }

    //Taking the amount or K from user input from argument 2 and create random clusters
    public static void generateCentroids(String[] args, Configuration conf, Path out, List<Integer> col1, List<Integer> col2) throws
            IOException, InterruptedException{
        SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(out);
        SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
        SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(IntWritable.class);
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf, opPath, opKey, opValue);
        final IntWritable value = new IntWritable(0);
        for (int i = 0; i < Integer.valueOf(args[2]); i++){
            Random r = new Random();
            centerWriter.append(new Centroid(new DataPoint(r.nextInt(Collections.max(col1)), r.nextInt(Collections.max(col2)))), value);
        }
        IOUtils.closeStream(centerWriter);
    }
}
