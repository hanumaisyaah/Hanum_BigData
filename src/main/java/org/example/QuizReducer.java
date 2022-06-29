package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.mortbay.util.IO;

import java.io.IOException;
import java.util.Iterator;

public class QuizReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException
    {
        System.out.println("------------------------");
        System.out.println("Ini dari reducer");
        System.out.println("Title: " + key.toString());
        System.out.println("------------------------");
        int sum = 0;
        while(values.hasNext()){
            IntWritable currentRevenue = values.next();
            System.out.println(("Jumlah view: " + currentRevenue.get()));
            sum += currentRevenue.get();
        }
        output.collect(key, new IntWritable(sum));
    }
}
