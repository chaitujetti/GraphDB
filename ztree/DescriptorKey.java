package ztree;

import global.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**  DescriptorKey: It extends the KeyClass.
 *   It defines the Descriptor Key.
 */
public class DescriptorKey extends KeyClass {

    private BigInteger key; //Bit Shuffled Descriptor
    private Descriptor desc;

    /** Class constructor
     *  @param     desc   the value of the integer key to be set
     *
     */
    public DescriptorKey(Descriptor desc)
    {
        short[] values=getValues(desc);
        int andValue=16384;
        int rightShiftValue=15;
        this.desc=new Descriptor();
        this.desc.set((short)desc.get(0),(short)desc.get(1),(short)desc.get(2),(short)desc.get(3),(short)desc.get(4));
        key=BigInteger.ZERO;

        for (int i=0;i<16;i++)
        {
            for(int j=0;j<5;j++) {
                int bitVal = values[j] & andValue;
                bitVal = bitVal >> rightShiftValue;
                BigInteger b=BigInteger.valueOf(bitVal);
                key.shiftLeft(1).or(b);
            }
            andValue = andValue >> 1;
            rightShiftValue -= 1;
        }
    }

//    public DescriptorKey(BigInteger B_key){
//        key=new BigInteger(B_key.toString());
//    }

    public Descriptor getDesc()
    {
        return this.desc;
    }
    public String toString(){
        return key.toString();
    }

    public short[] getValues(Descriptor desc) //Help constructor get a short[] from Descriptor
    {
        short[] values=new short[5];
        for (int i=0;i<5;i++)
            values[i]=(short)desc.get(i);
        return values;
    }

    /** get a copy of the integer key
     *  @return the reference of the copy
     */
    public BigInteger getKey()
    {
            return key;
    }

    /** set the integer key value
     */
    public void setKey(BigInteger B_key)
    {
        key=new BigInteger(B_key.toString());
    }
}