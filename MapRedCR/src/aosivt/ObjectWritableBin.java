package aosivt;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by alex on 29.09.15.
 */
public class ObjectWritableBin implements Writable {
    private byte[] data;
    private String index;
    private String NameCanal;


    public void write(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(NameCanal);
        out.write(data);

    }


    public void readFields(DataInput in) throws IOException {
        index = in.readUTF();
        NameCanal = in.readUTF();

        DataInputBuffer bis = (DataInputBuffer) in;

        byte[] buffer = new byte[bis.available()];
        in.readFully(buffer);
        data = buffer;

    }
    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;

    }
    public void setIndex(String index) {
        this.index = index;
    }
    public String getIndex() {
        return index;
    }

    public String getNameCanal() {
        return NameCanal;
    }

    public void setNameCanal(String nameCanal) {
        this.NameCanal = nameCanal;
    }

}