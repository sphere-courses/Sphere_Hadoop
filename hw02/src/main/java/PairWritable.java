import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PairWritable<
        firstType extends WritableComparable<BinaryComparable>,
        secondType extends WritableComparable<BinaryComparable>
        > implements WritableComparable<PairWritable<firstType, secondType>> {
    private firstType first;
    private secondType second;

    public PairWritable(){
        first = (firstType) new Text();
        second = (secondType) new Text();
    }

    public PairWritable(firstType first, secondType second){
        this.first = first;
        this.second = second;
    }

    public firstType getFirst(){
        return first;
    }

    public secondType getSecond(){
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws  IOException{
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int compareTo(@Nonnull PairWritable other) {
        int tmp = first.toString().compareTo(other.first.toString());
        return tmp == 0 ? second.toString().compareTo(other.second.toString()) : tmp;
    }

    @Override
    public boolean equals(Object object){
        if(object instanceof PairWritable){
            PairWritable other = (PairWritable)object;
            return first.equals(other.getFirst()) && second.equals(other.getSecond());
        }
        return false;
    }

    @Override
    public String toString(){
        return first.toString() + '\t' + second.toString();
    }

    @Override
    public int hashCode(){
        // name's hashCode is multiplied by an arbitrary prime number (17)
        // in order to make sure there is a difference in the hashCode between
        // these two parameters:
        //  name: a  value: aa
        //  name: aa value: a
        return first.hashCode() * 17 + second.hashCode();
    }


    public static class PairWritableCompator extends WritableComparator {
        protected PairWritableCompator() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return left.compareTo(right);
        }
    }

    public static class PairWritableGroupCompator extends WritableComparator {
        protected PairWritableGroupCompator() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return ((PairWritable) left).getFirst().compareTo(((PairWritable) right).getFirst());
        }
    }

}
