import java.text.DecimalFormat;
import java.util.*;

import PCP.PCPBroker;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import utils.Accountant;
import utils.Task;
import utils.WorkflowType;


public class Scheduler {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		Log.printLine("Starting CloudSimExample...");

		try {
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
 			boolean trace_flag = false; // trace events

			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("My_Datacenter");


			// Third step: Create Broker
			DatacenterBroker broker = createMyBroker("My_Broker",
					"/Users/hossein/IdeaProjects/CloudSim/workflowData/CyberShake_500_1.xml", WorkflowType.CYBER_SHAKE);
//					"/Users/hossein/IdeaProjects/CloudSim/workflowData/LIGO_500_1.xml", WorkflowType.LIGO);
//					"/Users/hossein/IdeaProjects/CloudSim/workflowData/Montage_500_1.xml", WorkflowType.MONTAGE);
//					"/Users/hossein/IdeaProjects/CloudSim/workflowData/SIPHT_500_1.xml", WorkflowType.SIPHT);
//					"/Users/hossein/IdeaProjects/CloudSim/workflowData/testWorkflow2.xml", WorkflowType.TEST);
//					"/Users/hossein/IdeaProjects/CloudSim/workflowData/testWorkflow.xml", WorkflowType.TEST);

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			List<Cloudlet> newList = broker.getCloudletReceivedList();


			CloudSim.stopSimulation();
//
			Collections.sort(newList, (t1, t2) ->
					Double.compare(t1.getExecStartTime(), t2.getExecStartTime()));

			printCloudletList(newList);

			Log.printLine("CloudSimExample finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		long mips = 1_000_000_000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating


		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 16 * 2048; // host memory (MB)
		long storage = 100_000_000; // host storage
		int bw = 100000;

		int hostNumber = 4;

		for (int i = 0 ; i < hostNumber; i++) {
			hostList.add(
					new Host(
							hostId + i,
							new RamProvisionerSimple(ram),
							new BwProvisionerSimple(bw),
							storage,
							peList,
							new VmSchedulerTimeShared(peList)
					)
			); // This is our machine
		}


		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$w/Pe time unit).
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
										// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
													// devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" +
				indent + "Data center ID" +
				indent + "VM ID" +
				indent + indent + "Time" +
				indent + "Start Time" +
				indent + "Finish Time" +
				indent + "price"
		);

		DecimalFormat dft = new DecimalFormat("###.##");
		dft.setMinimumIntegerDigits(2);
		double total_run_time = 0;
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + dft.format(cloudlet.getCloudletId()) + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + dft.format(cloudlet.getResourceId()) +
						indent + indent + indent + dft.format(cloudlet.getVmId()) +
						indent + indent + dft.format(cloudlet.getActualCPUTime()) +
						indent + indent + dft.format(cloudlet.getExecStartTime()) +
						indent + indent + indent + dft.format(cloudlet.getFinishTime()) +
						indent + indent + indent + dft.format(Accountant.getInstance().getCash((cloudlet))) + "$")
						;

				total_run_time = cloudlet.getFinishTime();
			}
		}
		Log.printLine("total cash is : " + Accountant.getInstance().getTotalCash() + "$");
		Log.printLine("total time is : " + total_run_time + "ms");
	}

	private static DatacenterBroker createMyBroker(String name, String path, WorkflowType workflowType) {
		DatacenterBroker broker = null;
		try {
			broker = new PCPBroker(name, path, workflowType);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}


}