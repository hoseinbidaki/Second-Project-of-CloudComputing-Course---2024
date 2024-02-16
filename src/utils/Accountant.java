package utils;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;

import java.util.HashMap;

public class Accountant {
    private static Accountant accountant;
    public static Accountant getInstance()
    {
        if (accountant == null) accountant = new Accountant();
        return accountant;
    }

    private HashMap<Cloudlet, Double> cash = new HashMap<>();

    public void use(Cloudlet cloudlet, Vm vm)
    {
        switch ((int) vm.getMips())
        {
            case 500: cash.put(cloudlet, 1.5);break;
            case 1000: cash.put(cloudlet, 2.25);break;
            case 1500: cash.put(cloudlet, 3.5);break;
            case 2000: cash.put(cloudlet, 5.0);break;
            default:break;
        }
    }

    public double getCash(Cloudlet cloudlet) {
        return cash.get(cloudlet);
    }

    public double getTotalCash()
    {
        double sum = 0;
        for (Cloudlet cloudlet : cash.keySet())
            sum += cash.get(cloudlet);
        return sum;
    }
}
