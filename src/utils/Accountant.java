package utils;

import org.cloudbus.cloudsim.Vm;

import java.util.ArrayList;
import java.util.HashMap;

public class Accountant {

    private HashMap<Vm, Integer> numberOfUsed;

    private  HashMap<Vm, Double> prices;

    private  HashMap<Vm, ArrayList<Task>> used;

    public Accountant()
    {
        numberOfUsed = new HashMap<>();
        prices = new HashMap<>();
        used = new HashMap<>();
    }


    private static Accountant accountant = null;

    public static Accountant getInstance()
    {
        if (accountant == null) {
            accountant = new Accountant();
        }
        return accountant;
    }

    public Double getPrice(Vm v)
    {
        return prices.get(v);
    }

    public void setPrice(ArrayList<Vm> vms)
    {
        for (Vm v : vms)
        {
//            int mips1 = 500;
//            int mips2 = 1000;
//            int mips3 = 1500;
//            int mips4 = 2000;
            double price = Double.MAX_VALUE;
            if (v.getMips() == 500)
            {
                price = 0.5;
            }
            else if (v.getMips() == 1000)
            {
                price = 0.75;
            }
            else if (v.getMips() == 1500)
            {
                price = 1.25;
            }
            else if (v.getMips() == 2000)
            {
                price = 1.5;
            }
            prices.put(v, price);
        }
    }
}
