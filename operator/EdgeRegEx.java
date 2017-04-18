package operator;


import global.Descriptor;


public class EdgeRegEx {

    private String label;
    private int max_edge_weight;

    public EdgeRegEx(String label) {
        this.label = label;
        this.max_edge_weight = -1;
    }

    public EdgeRegEx(int max_edge_weight)
    {
        this.label = null;
        this.max_edge_weight = max_edge_weight;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getMax_edge_weight() {
        return max_edge_weight;
    }

    public void setMax_edge_weight(int max_edge_weight) {
        this.max_edge_weight = max_edge_weight;
    }
}
