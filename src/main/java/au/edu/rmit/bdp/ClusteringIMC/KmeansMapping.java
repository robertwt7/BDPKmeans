package au.edu.rmit.bdp.ClusteringIMC;

import au.edu.rmit.bdp.Distances.DistanceMeasurer;
import au.edu.rmit.bdp.Distances.EuclidianDistance;
import au.edu.rmit.bdp.model.Centroid;
import au.edu.rmit.bdp.model.DataPoint;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//last generic that specifies the output value is changed to assoc array
public class KmeansMapping extends Mapper<Centroid, DataPoint, Centroid, Text>{

    private final List<Centroid> centers = new ArrayList<>();
    private DistanceMeasurer distanceMeasurer;

    //Logging
    private static final Log LOG = LogFactory.getLog(App.class);

    private List<DataPoint> dp;

    //Initiate associative array
    Map<Centroid, List<DataPoint>> assocArray;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //Setup assoc array and list
        assocArray = getMap();
        dp = new ArrayList<DataPoint>();

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
                assocArray.put(centroid, dp);
            }
        }
        distanceMeasurer = new EuclidianDistance();
    }

    public Map<Centroid, List<DataPoint>> getMap() {
        if(null == assocArray) //lazy loading
            assocArray = new HashMap<Centroid, List<DataPoint>>();
        return assocArray;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Text value = new Text();
        StringBuilder builder = new StringBuilder();
        for (Centroid a : assocArray.keySet()){
            if (assocArray.get(a).toArray().length > 0){
                for (DataPoint point : assocArray.get(a)){
                    LOG.info("Association per point: " + point.toString());
                    builder.append(point).append(";");
                }
            }
            builder.setLength(builder.length() - 1);
            value.set(builder.toString());
            context.write(a, value);
        }

    }

    @Override
    protected void map(Centroid key, DataPoint value, Context context) throws IOException, InterruptedException {
        assocArray = getMap();

        Centroid nearest = null;
        double nearestDistance = Double.MAX_VALUE;

        for (Centroid c : centers){
            double distance = distanceMeasurer.measureDistance(c.getCenterVector(), value.getVector());
            if (nearest == null) {
                nearest = c;
                nearestDistance = distance;
            }
            else {
                if (nearestDistance > distance) {
                    nearest = c;
                    nearestDistance = distance;
                }
            }
        }


        //Fill assoc array with list
        dp = assocArray.get(nearest);
        dp.add(value);
        assocArray.put(nearest,dp);

        //Does not called in IMC
        //context.write(nearest, value);

    }
}
