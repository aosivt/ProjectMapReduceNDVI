package aosivt;

/**
 * Created by alex on 08.11.15.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author alex
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HaLongWritable implements WritableComparable<HaLongWritable>
{
    private int value;
    public HaLongWritable()
    {
        value = 0;
    }


    public int get() {return value;}
    public void set( int o) {value = o;}

    @Override
    public void write( DataOutput out ) throws IOException
    {
        out.writeInt( value );
    }

    @Override
    public void readFields( DataInput in ) throws IOException
    {
        value = in.readInt();
    }

    @Override
    public int compareTo( HaLongWritable o )
    {
        return ((int)(this.value) - (int)(o.get()));/*сравнение по модулю*/
        //return ((int)(this.value) - (int)(o.get()));
        //return (this.value < o.get() ? -1 : (this.value == o.get() ? 0 : 1 ));
    }
    @Override
    public int hashCode()
    {
        int temp = 0;
/*
        temp = (int)(value) <= 0 && (int)(value) <1000 ? (int)(value) : 1000;
        temp = (int)(value) <= 1000 && (int)(value) <2000 ? (int)(value) : 2000;
        temp = (int)(value) <= 2000 && (int)(value) <3000 ? (int)(value) : 3000;
        temp = (int)(value) <= 3000 && (int)(value) <4000 ? (int)(value) : 4000;
        temp = (int)(value) <= 4000 && (int)(value) <5000 ? (int)(value) : 5000;
        temp = (int)(value) <= 5000 && (int)(value) <6000 ? (int)(value) : 6000;
        temp = (int)(value) <= 6000 ? (int)(value) : 7000;
        */

        if      (0<=    (int)(value) && (int)(value)<1000)    {temp = 7000;}
        else if (1000<= (int)(value) && (int)(value)<2000)    {temp = 6000;}
        else if (2000<= (int)(value) && (int)(value)<3000)    {temp = 5000;}
        else if (3000<= (int)(value) && (int)(value)<4000)    {temp = 4000;}
        else if (4000<= (int)(value) && (int)(value)<5000)    {temp = 3000;}
        else if (5000<= (int)(value) && (int)(value)<6000)    {temp = 2000;}
        else if (6000<= (int)(value))                         {temp = 1000;}
        return temp;

        //return 2 + (int)(value %2); /*сравнение по модулю*/
        //return (int)(value); /*простое сравнение*/

    }

    @Override
    public boolean equals( Object obj )
    {
        if( obj == null )
        {
            return false;
        }
        if( getClass() != obj.getClass() )
        {
            return false;
        }
        final HaLongWritable other = ( HaLongWritable ) obj;
        return (value) == (other.get()); /*сравнение по модулю*/
        //return (value) == (other.get());
    }
}


