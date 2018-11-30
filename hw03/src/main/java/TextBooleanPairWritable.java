import org.apache.hadoop.io.*;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextBooleanPairWritable implements WritableComparable<TextBooleanPairWritable>{
    private String first;
    private boolean second;

    public TextBooleanPairWritable(){
    }

    public TextBooleanPairWritable(String first, boolean second){
        this.first = first;
        this.second = second;
    }

    public String getFirst(){
        return first;
    }

    public boolean getSecond(){
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeBoolean(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws  IOException{
        first = dataInput.readUTF();
        second = dataInput.readBoolean();
    }

    @Override
    public int compareTo(@Nonnull TextBooleanPairWritable other) {
        int tmp = first.compareTo(other.first);
        if(tmp == 0){
            if(second == other.second) {
                return 0;
            }
            if(second) {
                return -1;
            }
            return 1;
        }
        return tmp;
    }


    @Override
    public int hashCode(){
        return first.hashCode();
    }


    public static class PairWritableCompator extends WritableComparator {
        protected PairWritableCompator() {
            super(TextBooleanPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return left.compareTo(right);
        }
    }

    public static class PairWritableGroupCompator extends WritableComparator {
        protected PairWritableGroupCompator() {
            super(TextBooleanPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return ((TextBooleanPairWritable) left).getFirst().compareTo(((TextBooleanPairWritable) right).getFirst());
        }
    }

}
