package au.edu.rmit.bdp.ClusteringIMC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import au.edu.rmit.bdp.model.Centroid;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.rmit.bdp.model.DataPoint;
import de.jungblut.math.DoubleVector;

public class KmeansReducing extends Reducer<Centroid, Text, Centroid, DataPoint>{

    public enum Counter{
        CONVERGED
    }

    private final List<Centroid> centers = new ArrayList<>();

    //Logging
    private static final Log LOG = LogFactory.getLog(App.class);

    @Override
    protected void reduce(Centroid key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<DataPoint> vectorList = new ArrayList<>();
        DoubleVector newCenter = null;

        //get every of the data point based on specific centroid in the assoc array
        for (Text v : values) {
            String[] points = v.toString().split(";");
            for (String value : points){
                //String[] indexValues = values.toString().split(" -> ");
                //Taking every points in the bracket [ ] and put the value inside a new datapoints
                Pattern p = Pattern.compile("\\[(.*?)\\]");
                Matcher m = p.matcher(value);
                DataPoint temp = new DataPoint(0);
                while(m.find()) {
                    System.out.println(m.group(1));
                    String xy[] = m.group(1).split(",");
                    temp = new DataPoint(Double.valueOf(xy[0]), Double.valueOf(xy[1]));
                    vectorList.add(temp);
                    if (newCenter == null) {
                        newCenter = temp.getVector().deepCopy();
                    }
                    else
                        newCenter = newCenter.add(temp.getVector());
                }
            }
        }
        newCenter = newCenter.divide(vectorList.size());
        Centroid newCentroid = new Centroid(newCenter);
        centers.add(newCentroid);

        for (DataPoint vector : vectorList){
            context.write(newCentroid, vector);
        }
        if (newCentroid.update(key))
            context.getCounter(KmeansReducing.Counter.CONVERGED).increment(1);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Configuration conf = context.getConfiguration();
        Path outPath = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outPath, true);

        //writing options
        SequenceFile.Writer.Option opPath = SequenceFile.Writer.file(outPath);
        SequenceFile.Writer.Option opKey = SequenceFile.Writer.keyClass(Centroid.class);
        SequenceFile.Writer.Option opValue = SequenceFile.Writer.valueClass(IntWritable.class);
        try (SequenceFile.Writer out = SequenceFile.createWriter(conf, opPath, opKey, opValue)){
            final IntWritable value = new IntWritable(0);
            for(Centroid center : centers){
                out.append(center, value);
            }
        }
    }
}
