import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRankMRGetTop extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new PageRankMRGetTop(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir, String leakedPRFile) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageRankMRGetTop.class);
        job.setJobName(PageRankMRGetTop.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankMRGetTop.PageRankGetTopMapper.class);
        job.setReducerClass(PageRankMRGetTop.PageRankGetTopReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setLong("nPages", 4847571);
        job.getConfiguration().setDouble("d", 0.85);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path leakedPRPath = new Path(leakedPRFile);
        FSDataInputStream inputStream = fs.open(leakedPRPath);
        double leakedPR = Double.parseDouble(IOUtils.toString(inputStream, "UTF-8"));
        job.getConfiguration().setDouble("leakedPR", leakedPR);
        inputStream.close();
        fs.close();

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1], args[2]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PageRankGetTopMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        protected void map(Text nodeInfoText, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] nodeInfo = nodeInfoText.toString().split(",");

            double PR = Double.valueOf(nodeInfo[1]);

            int nPages = context.getConfiguration().getInt("nPages", 1);
            double d = context.getConfiguration().getDouble("d", 0.);
            double leakedPR = context.getConfiguration().getDouble("leakedPR", 0.);

            PR += leakedPR / nPages;

            context.write(new DoubleWritable(-PR), new Text(nodeInfo[0]));
        }

    }

    public static class PageRankGetTopReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        protected void reduce(DoubleWritable Pr, Iterable<Text> nodesId, Context context) throws IOException, InterruptedException {
            for(Text nodeId : nodesId){
                context.write(Pr, nodeId);
            }
        }
    }
}
