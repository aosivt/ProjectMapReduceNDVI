package aosivt;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Sortulka extends WritableComparator
{
    public Sortulka()
    {
        super( HaLongWritable.class, true );
    }
    @Override
    /*
    public int compare( WritableComparable k1, WritableComparable k2 ) {

        int a1 = ((HaLongWritable)k1).get();
        int a2 = ((HaLongWritable)k2).get();
        if( a1 > a2 ) return 1;
        if( a1 == a2 ) return 0;
        return -1;

        /*При смене знака у 1 и -1 меняется напрвления сортировки(убывание или возразстание соответственно)
    }
    */
    public int compare( WritableComparable k1, WritableComparable k2 ) {

        return 1*((HaLongWritable)k1).compareTo( ((HaLongWritable)k2));


        /*При смене знака у 1 и -1 меняется напрвления сортировки(убывание или возразстание соответственно)*/
    }
}