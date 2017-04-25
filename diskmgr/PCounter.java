package diskmgr;

public class PCounter {
    private static int rcounter;
    private static int wcounter;

    public static void initialize() {
        rcounter = 0;
        wcounter = 0;
    }

    public static void readIncrement() {
        rcounter++;
    }
    public static void writeIncrement() {
        wcounter++;
    }
    public static int getReadCount()
    {
        int readCount = rcounter;
        rcounter=0;
        return readCount;
    }
    public static int getWriteCount()
    {
        int writeCount = wcounter;
        wcounter=0;
        return writeCount;
    }

}
