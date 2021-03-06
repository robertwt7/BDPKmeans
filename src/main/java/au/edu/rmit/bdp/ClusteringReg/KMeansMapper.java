package au.edu.rmit.bdp.ClusteringReg;

import au.edu.rmit.bdp.Distances.DistanceMeasurer;
import au.edu.rmit.bdp.Distances.EuclidianDistance;
import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;





//last generic that specifies the output value is changed to assoc array
public class KMeansMapper extends Mapper<Centroid, DataPoint, Centroid, DataPoint> {


    private final List<Centroid> centers = new ArrayList<>();
    private DistanceMeasurer distanceMeasurer;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);


        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get("centroid.path"));
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader.Option opPath = SequenceFile.Reader.file(centroids);

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, opPath)){
            Centroid key = new Centroid();
            IntWritable value = new IntWritable();
            int index = 0;
            while (reader.next(key, value)){
                Centroid centroid = new Centroid(key);
                centroid.setClusterIndex(index++);
                centers.add(centroid);
            }
        }
        distanceMeasurer = new EuclidianDistance();
    }


    @Override
    protected void map(Centroid centroid, DataPoint dataPoint, Context context) throws IOException, InterruptedException {
        Centroid nearest = null;
        double nearestDistance = Double.MAX_VALUE;

        for (Centroid c : centers) {
            //done: find the nearest centroid for the current dataPoint, pass the pair to reducer
            double dist = distanceMeasurer.measureDistance(c.getCenterVector(), dataPoint.getVector());
            if (nearest == null) {
                nearest = c;
                nearestDistance = dist;
            } else {
                if (nearestDistance > dist) {
                    nearest = c;
                    nearestDistance = dist;
                }
            }
        }

        context.write(nearest, dataPoint);
    }
}

