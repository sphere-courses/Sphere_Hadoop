import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static org.apache.commons.lang.SerializationUtils.deserialize;
import static org.apache.commons.lang.SerializationUtils.serialize;

public class InflaterInputFormat extends FileInputFormat<LongWritable, Text> {
    public static class IdxFileSplit extends FileSplit {
        Vector<Integer> chunkSizes;

        public IdxFileSplit(){}

        private IdxFileSplit(Path file, long start, long length, String[] hosts, Vector<Integer> chunkSizes){
            super(file, start, length, hosts);
            this.chunkSizes = chunkSizes;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);

            byte dataSerialized[] = serialize(chunkSizes);
            out.writeInt(dataSerialized.length);
            out.write(dataSerialized);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            super.readFields(in);

            int dataSize = in.readInt();
            byte dataSerialized[] = new byte[dataSize];
            in.readFully(dataSerialized);
            chunkSizes = (Vector<Integer>) deserialize(dataSerialized);
        }
    }

    public class InflaterRecordReader extends RecordReader<LongWritable, Text>{
        FSDataInputStream inputFile;
        Vector<Integer> chunkSizes;
        int currentIdx = 0;
        long currentOffset = 0;
        long splitSize;

        long currentKey;
        String currentValue;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext taskAttemptContext) throws IOException{
            Configuration conf = taskAttemptContext.getConfiguration();

            IdxFileSplit fileSplit = (IdxFileSplit) split;

            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);

            inputFile = fileSystem.open(path);
            inputFile.seek(fileSplit.getStart());
            chunkSizes = fileSplit.chunkSizes;
            splitSize = fileSplit.getLength();
        }

        @Override
        public boolean nextKeyValue() throws IOException{
            if(currentIdx == chunkSizes.size()){
                return false;
            }
            try {
                currentValue = inflate(inputFile, chunkSizes.get(currentIdx));
            } catch (DataFormatException exp){
                return false;
            }
            currentOffset += chunkSizes.get(currentIdx++);
            return true;
        }

        @Override
        public LongWritable getCurrentKey(){
            return new LongWritable(currentOffset);
        }

        @Override
        public Text getCurrentValue(){
            return new Text(currentValue);
        }

        @Override
        public float getProgress(){
            return (float)currentKey / chunkSizes.size();
        }

        @Override
        public void close() throws IOException{
            inputFile.close();
        }

        private String inflate(FSDataInputStream dataFile, int size) throws IOException, DataFormatException {
            Inflater inflater = new Inflater();
            byte[] byteBuffer = new byte[size];

            int read, read_total = 0;
            while(read_total != size) {
                read = dataFile.read(byteBuffer, read_total, size - read_total);
                read_total += read;
            }
            inflater.setInput(byteBuffer);

            String result;
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                while (!inflater.finished()) {
                    int bytesInflated = inflater.inflate(buffer);
                    byteArrayOutputStream.write(buffer, 0, bytesInflated);
                }
                result = byteArrayOutputStream.toString("UTF-8");
            } finally {
                inflater.end();
            }
            return result;
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException{
        return new InflaterRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        Configuration conf = context.getConfiguration();

        for(FileStatus status : listStatus(context)){
            Path dataPath = status.getPath();
            Path idxPath = dataPath.suffix(".idx");

            FileSystem fileSystem = idxPath.getFileSystem(conf);
            FSDataInputStream inputStream = fileSystem.open(idxPath);

            long idxFileSize = fileSystem.getFileStatus(idxPath).getLen();
            long splitMaxSize = getNumBytesPerSplit(conf);

            byte[] byteArray = new byte[(int)idxFileSize];
            inputStream.readFully(byteArray);
            int[] chunkSizes = toIntArray(byteArray);

            long currentOffset = 0, currentSize = 0;
            Vector<Integer> splitChunkSizes = new Vector<>();
            for(int value : chunkSizes){
                if(currentSize + value <= splitMaxSize){
                    currentSize += value;
                    splitChunkSizes.add(value);
                } else {
                    splits.add(new IdxFileSplit(dataPath, currentOffset, currentSize, null, splitChunkSizes));

                    splitChunkSizes = new Vector<>();
                    splitChunkSizes.add(value);

                    currentOffset += currentSize;
                    currentSize = value;
                }
            }
            if(currentSize != 0) {
                splits.add(new IdxFileSplit(dataPath, currentOffset, currentSize, null, splitChunkSizes));
            }
        }
        return splits;
    }

    private static long getNumBytesPerSplit(Configuration conf) {
        return conf.getLong("mapreduce.input.indexedgz.bytespermap", 64 * 1024 * 1024);
    }

    private static int[] toIntArray(byte[] byteArray){
        final IntBuffer intBuffer =
                ByteBuffer.wrap(byteArray)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer();
        final int[] intArray = new int[intBuffer.remaining()];
        intBuffer.get(intArray);
        return intArray;
    }
}
