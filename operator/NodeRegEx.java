package operator;


import global.Descriptor;

/**
 * Created by bharath on 4/13/17.
 */
public class NodeRegEx {

    private String label;
    private Descriptor desc;

    public NodeRegEx(String label) {
        this.label = label;
        this.desc = null;
    }

    public NodeRegEx(Descriptor desc)
    {
        this.label = null;
        this.desc = desc;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Descriptor getDesc() {
        return desc;
    }

    public void setDesc(Descriptor desc) {
        this.desc = desc;
    }
}
