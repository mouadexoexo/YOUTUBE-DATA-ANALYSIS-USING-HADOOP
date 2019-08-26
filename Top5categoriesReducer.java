package top5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5categoriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  //  private IntWritable totalWordCount = new IntWritable();

    @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context)
         throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {

               sum += val.get();
           }
           context.write(key, new IntWritable(sum));
       }

       // totalWordCount.set(sum);
        // context.write(key, new IntWritable(sum));
        //context.write(key, totalWordCount);
    }

