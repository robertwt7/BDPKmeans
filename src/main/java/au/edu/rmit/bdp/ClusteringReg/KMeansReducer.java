package au.edu.rmit.bdp.ClusteringReg;

import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
import de.jungblut.math.DoubleVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducer extends Reducer<Centroid, DataPoint, Centroid, DataPoint> {

    public enum Counter2{
        CONVERGED
    }

    private final List<Centroid> centers = new ArrayList<>();

    @Override
    protected void reduce(Centroid key, Iterable<DataPoint> values, Context context) throws IOException, InterruptedException {
        List<DataPoint> vectorList = new ArrayList<>();
        DoubleVector newCenter = null;

        //get every of the data point based on specific centroid in the assoc array
        for (DataPoint value : values) {
            vectorList.add(new DataPoint(value));
            if (newCenter == null)
                newCenter = value.getVector().deepCopy();
            else
                newCenter = newCenter.add(value.getVector());
        }
        newCenter = newCenter.divide(vectorList.size());
        Centroid newCentroid = new Centroid(newCenter);
        centers.add(newCentroid);

        for (DataPoint vector : vectorList){
            context.write(newCentroid, vector);
        }
        if (newCentroid.update(key))
            context.getCounter(KMeansReducer.Counter2.CONVERGED).increment(1);
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
