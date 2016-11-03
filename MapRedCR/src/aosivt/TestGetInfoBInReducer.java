package aosivt;

/**
 * Created by alex on 29.09.15.
 */


import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by alex on 27.09.15.
 */
public class TestGetInfoBInReducer extends Reducer<HaLongWritable,ObjectWritableBin,Text,Text> {

    private Text result = new Text();
    private int numRow = 1;


    public void reduce(HaLongWritable key, Iterable<ObjectWritableBin> values,
                       Context context
    ) throws IOException, InterruptedException {

        float[] _date = null;
        String str = "";
        String[] arr = new String[2];


        for (ObjectWritableBin custBin : values) {
            ///Преобразование byte[]  в объект
            ByteArrayInputStream bisBuf = new ByteArrayInputStream(custBin.getData());
            ObjectInput inOb = null;

            try {
                inOb = new ObjectInputStream(bisBuf);
                Object o = inOb.readObject();
                str = (String) o;
                inOb.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            if (arr[0] == null || arr[1] == null)
            {
                if (custBin.getNameCanal().indexOf("B3") > 0)
                    {arr[0] = str.replace(" ","");
                        str = custBin.getNameCanal().
                                substring(0,custBin.getNameCanal().
                                        indexOf("B3"));}
                else
                if (custBin.getNameCanal().indexOf("B4") > 0)
                    {arr[1] = str.replace(" ","");
                        str = custBin.getNameCanal().
                                substring(0,custBin.getNameCanal().
                                        indexOf("B4"));}
            }
            if (arr[0] != null && arr[1] != null) {
/*
                if (numRow != Integer.getInteger(custBin.getIndex()) +1 )
                {
                    System.out.println(timeFormat.format(new Date(System.currentTimeMillis())) + custBin.getIndex());
                }
                else
                {
                    numRow = Integer.getInteger(custBin.getIndex());
                }
                */
                result = new Text(str + Arrays.toString(funcReseachNDVI(arr[0].split(","), arr[1].split(",")))
                                  .replace("[","").replace("]",""));
                key.set(Integer.parseInt(custBin.getIndex()));

                context.write(new Text(String.valueOf(key.get())), result);
                SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

                System.out.println(timeFormat.format(new Date(System.currentTimeMillis())) +"!!!" + custBin.getIndex());

                str = "";
                arr[0] = null;
                arr[1] = null;

            }


        }

    }

    public float[] funcReseachNDVI(String[] _red, String[] _nir)
    {
        float[] result = new float[_red.length];
        for (int ind=0;ind<_red.length;ind++)
        {
            result[ind] = Float.valueOf((Integer.parseInt(_nir[ind]) - Integer.parseInt(_red[ind])))/
                          Float.valueOf((Integer.parseInt(_nir[ind]) + Integer.parseInt(_red[ind])));
            if  (Float.isNaN(result[ind]))
                {result[ind] = (float) 0.0;}
        }
        return  result;
    }
}
