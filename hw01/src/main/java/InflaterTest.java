import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.util.zip.Inflater;

public class InflaterTest {
    public static void main(String[] args) throws Exception {

        String idxPath = "data/11e6c777-38e4-4584-86f5-2e50c4144854.pkz.idx";
        String dataPath = "data/11e6c777-38e4-4584-86f5-2e50c4144854.pkz";

        File idxFile = new File(idxPath);
        RandomAccessFile dataFile = new RandomAccessFile(dataPath, "r");

        byte[] byteArray = Files.readAllBytes(idxFile.toPath());
        int[] intArray = toIntArray(byteArray);
        for(int val : intArray) {
            System.out.println(inflate(dataFile, val));
        }
    }

    static private int[] toIntArray(byte[] byteArray){
        final IntBuffer intBuffer =
                ByteBuffer.wrap(byteArray)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer();
        final int[] intArray = new int[intBuffer.remaining()];
        intBuffer.get(intArray);
        return intArray;
    }

    static private String inflate(RandomAccessFile dataFile, int size) throws Exception{
        Inflater inflater = new Inflater();
        byte[] byteBuffer = new byte[size];
        dataFile.read(byteBuffer, 0, size);
        inflater.setInput(byteBuffer);

        String result;
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[64];
            while (!inflater.finished()) {
                int bytesInflated = inflater.inflate(buffer);
                byteArrayOutputStream.write(buffer, 0, bytesInflated);
            }
            result = byteArrayOutputStream.toString();
        } finally {
            inflater.end();
        }

        return result;
    }
}
