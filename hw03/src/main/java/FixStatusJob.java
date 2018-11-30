import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class FixStatusJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new FixStatusJob(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Job GetJobConf(Configuration conf, String webPagesPath, String webSitesPath) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(FixStatusJob.class);
        job.setJobName(FixStatusJob.class.getCanonicalName());

        List<Scan> inputTables = new ArrayList<>();

        job.getConfiguration().set("webPagesPath", webPagesPath);
        job.getConfiguration().set("webSitesPath", webSitesPath);

        inputTables.add(new Scan());
        inputTables.add(new Scan());
        inputTables.get(0).setAttribute("scan.attributes.table.name", Bytes.toBytes(webPagesPath));
        inputTables.get(1).setAttribute("scan.attributes.table.name", Bytes.toBytes(webSitesPath));

        TableMapReduceUtil.initTableMapperJob(
                inputTables,
                FixStatusMapper.class,
                TextBooleanPairWritable.class,
                Text.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                webPagesPath,
                FixStatusReducer.class,
                job
        );

        job.setNumReduceTasks(2);

        job.setPartitionerClass(FixStatusPartitioner.class);
        job.setSortComparatorClass(TextBooleanPairWritable.PairWritableCompator.class);
        job.setGroupingComparatorClass(TextBooleanPairWritable.PairWritableGroupCompator.class);

        return job;
    }

    public static class FixStatusPartitioner extends Partitioner<TextBooleanPairWritable, Text> {
        @Override
        public int getPartition(TextBooleanPairWritable key, Text val, int num_partitions) {
            int hashCode = key.hashCode();
            hashCode = hashCode > 0 ? hashCode : -hashCode;
            return hashCode % num_partitions;
        }
    }

    public static class FixStatusMapper extends TableMapper<TextBooleanPairWritable, Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            TableSplit tableSplit = (TableSplit)context.getInputSplit();
            String tableName = new String(tableSplit.getTableName());

            if(tableName.equals(context.getConfiguration().get("webPagesPath"))){
                byte[] url = columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("url"));
                byte[] isDisabled = columns.getValue(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));

                String urlString = new String(url);
                String host = new URLPreparer(urlString).getHost();
                if(isDisabled != null){
                    urlString = "Y_" + urlString;
                }
                context.write(new TextBooleanPairWritable(host, false), new Text(urlString));
            } else if(tableName.equals(context.getConfiguration().get("webSitesPath"))) {
                byte[] site = columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("site"));
                byte[] robots = columns.getValue(Bytes.toBytes("info"), Bytes.toBytes("robots"));

                String siteString = new String(site), robotsString;
                String host = new URLPreparer(siteString).getHost();
                if(robots != null){
                    robotsString = new String(robots);
                    context.write(new TextBooleanPairWritable(host, true), new Text("@" + robotsString));
                }
            }
        }
    }

    public static class FixStatusReducer extends TableReducer<TextBooleanPairWritable, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(TextBooleanPairWritable key, Iterable<Text> objects, Context context) throws IOException, InterruptedException {
            String line;
            URLFilter URLFilter = null;
            boolean wasDisallowed = false, isDisallowed = false;

            for(Text text : objects){
                line = text.toString();

                if(line.startsWith("@")){
                    URLFilter = new URLFilter(line.substring(1));
                    continue;
                }
                if(line.startsWith("Y_")){
                    wasDisallowed = true;
                    line = line.substring("Y_".length());
                }
                URLPreparer urlPreparer = new URLPreparer(line);

                if(URLFilter != null) {
                    isDisallowed = !URLFilter.IsAllowed(urlPreparer.getPath());
                }

                if(isDisallowed && !wasDisallowed){
                    Put put = new Put(urlPreparer.getMD5Hash());
                    put.add(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                    context.write(null, put);
                } else if(!isDisallowed && wasDisallowed) {
                    Delete delete = new Delete(urlPreparer.getMD5Hash());
                    delete.deleteColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                    context.write(null, delete);
                }
                wasDisallowed = false;
                isDisallowed = false;
            }
        }
    }

    private static class URLPreparer{
        private byte[] _md5;
        private String _host;
        private String _path = "";

        URLPreparer(String url) throws InterruptedException {
            calculateMD5Hash(url);

            int protoPos = url.indexOf("://");
            if(protoPos != -1){
                url = url.substring(protoPos + "://".length());
            }

            String[] parts = url.split("/", 2);
            _host = parts[0];
            if(parts.length == 2){
                _path = parts[1];
                if(!_path.startsWith("/")){
                    _path = "/" + _path;
                }
            }
        }

        String getHost() {
            return _host;
        }

        String getPath() {
            return _path;
        }

        private void calculateMD5Hash(String url) throws InterruptedException {
            byte[] bytesOfMessage = url.getBytes();

            MessageDigest messageDigest;
            try {
                messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException exc) {
                throw new InterruptedException(exc.getMessage());
            }
            byte[] digest = messageDigest.digest(bytesOfMessage);
            _md5 = DatatypeConverter.printHexBinary(digest).toLowerCase().getBytes();
        }

        byte[] getMD5Hash() {
            return _md5;
        }
    }

}
