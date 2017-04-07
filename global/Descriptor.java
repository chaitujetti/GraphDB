package global;

import static java.lang.Math.sqrt;

/**
 * Created by vamsikrishnag on 3/7/17.
 */
public class Descriptor {
    short[] value = new short[5];
    public Descriptor()
    {
        value[0] = 0;
        value[1] = 0;
        value[2] = 0;
        value[3] = 0;
        value[4] = 0;
    }

    public void set(short value0, short value1, short value2, short value3, short value4) {
        value[0] = value0;
        value[1] = value1;
        value[2] = value2;
        value[3] = value3;
        value[4] = value4;
    }

    public int get(int idx) {
        return value[idx];
    }

    public double equal (Descriptor desc) {
        for(int i = 0; i < 5; i++){
            if(this.value[i] != desc.value[i]){
                return 0;
            }
        }
        return 1;
    }

    public double distance (Descriptor desc) {
        int dist = 0;
        for(int i = 0; i < 5; i++){
            dist = dist + ((this.value[i] - desc.value[i]) * (this.value[i] - desc.value[i]));
        }
        return sqrt(dist);
    }
}
