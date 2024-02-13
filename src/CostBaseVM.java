import sun.misc.VM;

public class CostBaseVM extends VM {
    private double cost;
    public CostBaseVM() {
        super();
    }

    public double getCostPerSecond()
    {
        return cost / 60;
    }
}
