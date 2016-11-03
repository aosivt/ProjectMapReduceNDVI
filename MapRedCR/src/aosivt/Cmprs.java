package aosivt;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by alex on 09.11.15.
 */
public class Cmprs extends WritableComparator
{
    public Cmprs()
    {
        super( HaLongWritable.class, true );
    }
    @Override
  /*
    public int compare( WritableComparable k1, WritableComparable k2 )
    {
        int a1 = (int)(((HaLongWritable)k1).get());
        int a2 = (int)(((HaLongWritable)k2).get());
        return a1 - a2; // возможные значения -1, 0, 1
        //if( a1 == a2 ) return 0;
        //return Integer.signum( a2 - a1);
    }
    */
    public int compare( WritableComparable k1, WritableComparable k2 )
    {
        return 1*((HaLongWritable)k1).compareTo( ((HaLongWritable)k2));
    }
}