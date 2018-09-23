package au.edu.rmit.bdp.customdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KmeansMapping extends Mapper<LongWritable,Text,Text,Text> {
}
