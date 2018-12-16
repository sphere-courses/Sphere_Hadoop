import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class PageRankMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int nIterations = Integer.valueOf(args[0]), exitCode = 0;
        for(int iter = 0; iter < nIterations; ++iter){
            exitCode = ToolRunner
                    .run(
                            new PageRankMR(),
                            new String[]{
                                    args[1] + '/' + String.valueOf(iter) + "/part-r-*",
                                    args[1] + '/' + String.valueOf(iter + 1) + '/',
                                    args[1] + "/leakedPR/" + String.valueOf(iter),
                                    args[1] + "/leakedPR/" + String.valueOf(iter + 1)
                            }
                    );
            if(exitCode != 0){
                System.exit(exitCode);
            }
        }
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir, String leakedPRInputFile, String leakedPROutputFile) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageRankMR.class);
        job.setJobName(PageRankMR.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankMRMapper.class);
        job.setReducerClass(PageRankMRReducer.class);
        job.setNumReduceTasks(32);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setLong("nPages", 4847571);
        job.getConfiguration().setDouble("d", 0.85);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path leakedPRPath = new Path(leakedPRInputFile);
        FSDataInputStream inputStream = fs.open(leakedPRPath);
        double leakedPR = Double.parseDouble(IOUtils.toString(inputStream, "UTF-8"));
        job.getConfiguration().setDouble("leakedPR", leakedPR);
        job.getConfiguration().set("leakedPRFile", leakedPROutputFile);
        inputStream.close();
        fs.close();

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1], args[2], args[3]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PageRankMRMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text fromId, Text toIds, Context context) throws IOException, InterruptedException {
            String[] fromData = fromId.toString().split(",");
            String[] toData = toIds.toString().split(",");

            long nPages = context.getConfiguration().getLong("nPages", 1);
            double leakedPR = context.getConfiguration().getDouble("leakedPR", 0.);

            long currentNPages = toData[0].equals("") ? 1 : toData.length;
            double currentPR = Double.parseDouble(fromData[1]) + leakedPR / nPages;

            Text toPR = new Text("P" + String.valueOf(currentPR / currentNPages));
            if(toData[0].equals("")){
                context.write(new Text(fromData[0]), new Text(""));
                context.write(new Text("-1"), toPR);
            } else {
                for(String toId : toData){
                    context.write(new Text(fromData[0]), new Text(toId));
                    context.write(new Text(toId), toPR);
                }
            }

        }
    }

    public static class PageRankMRReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text fromId, Iterable<Text> objects, Context context) throws IOException, InterruptedException {
            if(fromId.toString().equals("-1")){
                double leakedPR = 0.;
                for(Text object : objects){
                    leakedPR += Double.parseDouble(object.toString().substring(1));
                }
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path leakedPRPath = new Path(context.getConfiguration().get("leakedPRFile"));
                FSDataOutputStream outputStream = fs.create(leakedPRPath);
                outputStream.writeBytes(Double.toString(leakedPR));
                outputStream.close();
            } else {
                double PR = 0.;
                StringBuilder value = new StringBuilder();
                for(Text object : objects){
                    if(object.toString().startsWith("P")){
                        PR += Double.parseDouble(object.toString().substring(1));
                    } else {
                        value.append(object.toString()).append(",");
                    }
                }
                long nPages = context.getConfiguration().getLong("nPages", 1);
                double d = context.getConfiguration().getDouble("d", 0.);
                PR = (1 - d) / nPages + d * PR;

                String key = fromId.toString() + "," + String.valueOf(PR);
                if(value.length() == 0) {
                    context.write(new Text(key), new Text(""));
                } else {
                    context.write(new Text(key), new Text(value.substring(0, value.length() - 1)));
                }
            }
        }
    }

}
