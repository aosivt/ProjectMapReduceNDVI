package aosivt;

/**
 * Created by alex on 29.09.15.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Created by alex on 27.09.15.
 */
public class TestGetInfoBInMapper extends Mapper<Object, Text, HaLongWritable, ObjectWritableBin> {

    private ObjectWritableBin custBin = new ObjectWritableBin();
    private HaLongWritable index = new HaLongWritable();
    public int countred = 7;

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String var[] = new String[6];
        var = value.toString().split(";");
        Object inOb = null;
        if(var.length ==  3){

            index.set(Integer.parseInt(String.valueOf(var[0])));
            //custBin.setIndex(var[0]);
            custBin.setIndex(String.valueOf(var[0]));
            custBin.setNameCanal(var[1]);

            //inOb = var[1].split(",");
            inOb = var[2];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(inOb);///записывает любой объект

            } catch (IOException e) {
                e.printStackTrace();
            }
            custBin.setData(bos.toByteArray());

            context.write(index, custBin);
        }
    }
}