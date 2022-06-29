package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class QuizMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private IntWritable quizWritable = new IntWritable(0);
    private Text TitleText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();

        System.out.println(line);

        // Pecah dulu berdasarkan '-
        String[] split = line.split("\t");
        String title = split[3];

        //Cari revenue-nya
        String viewSplit = split[7];

        //Laporkan ke kolektor
        this.TitleText.set(title);
        this.quizWritable.set(Integer.parseInt(viewSplit));
        output.collect(this.TitleText, this.quizWritable);
    }
}

