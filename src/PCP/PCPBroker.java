package PCP;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import utils.*;

import javax.sound.midi.Soundbank;
import java.io.File;
import java.util.*;

class CriticalPath {

    protected List<Task> path;

    protected long totalRuntime = 0;

    public CriticalPath(List<Task> path) {
        this.path = path;
    }

    public List<Task> getPath() {
        return path;
    }

    public void setPath(List<Task> path) {
        this.path = path;
    }

    public long getTotalRuntime() {
        this.totalRuntime = 0;
        for (Task task : path){
            this.totalRuntime += task.getCloudletLength();
        }
        return totalRuntime;
    }

    public void setTotalRuntime(long totalRuntime) {
        this.totalRuntime = totalRuntime;
    }
}

public class PCPBroker extends DatacenterBroker {

    private int datacenterId = 2;

    private List<Task> cloudletList;

    private String daxPath;

    HashMap<Cloudlet, Long> subDeadLines = null;
    List<CriticalPath> criticalPaths = null;

    HashMap<CriticalPath, Long> remindTime = null;
    WorkflowType workflowType;

    /**
     * Created a new DatacenterBroker object.
     *
     * @param name         name to be associated with this entity (as required by Sim_entity class from
     *                     simjava package)
     * @param daxPath      daxPath refers to the xml file of workflow
     * @param workflowType workflowType associated with workflow type
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */

    public PCPBroker(String name, String daxPath, WorkflowType workflowType) throws Exception {
        super(name);
        this.daxPath = daxPath;
        this.workflowType = workflowType;
    }

    public void startEntity() {
        cloudletList = new ArrayList<Task>();

        File daxFile = new File(daxPath);
        if (!daxFile.exists()) {
            Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
        }
        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
        ReplicaCatalog.init(file_system);
        WorkflowParser parser = new WorkflowParser(getId(), daxPath);
        parser.parse();
        cloudletList = parser.getTaskList();
        submitCloudletList(parser.getTaskList());

        int dw = workflowType.value;
        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
//        int mips1 = 500;
//        int mips2 = 1000;
//        int mips3 = 1500;
//        int mips4 = 2000;
//        int mips5 = 2500;
        int mips1 = 2000;
        int mips2 = 3000;
        int mips3 = 4000;
        int mips4 = 5000;
        int mips5 = 6000;
        long bw = 1000;
        int nw = 7;
        int vms = (int) Math.ceil(nw / (14.0 * dw));
        System.out.println("@@@@@@ vms is : " + vms);
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name
        List<Vm> vmList  = new ArrayList<>();
        for (int i = 0; i < vms; i++) {
            Vm vm = new Vm(i, getId(), mips1, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmList.add(vm);
//			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }

        for (int i = vms; i < 2 * vms; i++) {
            Vm vm = new Vm(i, getId(), mips2, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmList.add(vm);
//			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }

        for (int i = 2 * vms; i < 3 * vms; i++) {
            Vm vm = new Vm(i, getId(), mips3, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmList.add(vm);
//			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }

        for (int i = 3 * vms; i < 4 * vms; i++) {
            Vm vm = new Vm(i, getId(), mips4, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmList.add(vm);
//			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }

        for (int i = 4 * vms; i < 5 * vms; i++) {
            Vm vm = new Vm(i, getId(), mips5, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmList.add(vm);
//			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
        }


        submitVmList(vmList);
//        Accountant.getInstance().setPrice(vmList);
        schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);

    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            // A finished cloudlet returned
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            // if the simulation finishes
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
            Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
                    + " has been created in Datacenter #" + datacenterId + ", Host #"
                    + VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
                    + " failed in Datacenter #" + datacenterId);
        }

        incrementVmsAcks();

        // all the requested VMs have been created
        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
            submitCloudlets();
        } else {
            // all the acks received, but some VMs were not created
            if (getVmsRequested() == getVmsAcks()) {
                // find id of the next datacenter that has not been tried
                for (int nextDatacenterId : getDatacenterIdsList()) {
                    if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
                        createVmsInDatacenter(nextDatacenterId);
                        return;
                    }
                }

                // all datacenters already queried
                if (getVmsCreatedList().size() > 0) { // if some vm were created
                    submitCloudlets();
                } else { // no vms created. abort
                    Log.printLine(CloudSim.clock() + ": " + getName()
                            + ": none of the required VMs could be created. Aborting");
                    finishExecution();
                }
            }
        }
    }

    protected boolean checkReady(Task task) {
        for (Task parent : task.getParentList())
        {
            if (!getCloudletReceivedList().contains(parent)) return false;
        }
        return true;
    }

    protected double getAverageMips(Cloudlet cloudlet) {
//        double sum_mips = 0;
//        for (Vm vm : getVmsCreatedList()) {
//            sum_mips += vm.getMips();
//        }
//        return 1.0 * sum_mips/ getVmsCreatedList().size();
        return cloudlet.getCloudletLength();
    }
    protected void setSubDeadlines(CriticalPath criticalPath, long D)
    {

        double maximum_mips = 0;
        for (Vm vm : getVmsCreatedList()) {
            maximum_mips = Math.max(maximum_mips, vm.getMips());
        }

        long total_run_time = 0;
        System.out.println("details of critical paths:");
        HashMap<Cloudlet, Long> fastest_run_times = new HashMap<>();
        for (Cloudlet cloudlet : criticalPath.getPath())
        {
            long average_run_time = cloudlet.getCloudletLength();
            long fastest_run_time = (long) ((getAverageMips(cloudlet) / maximum_mips) * average_run_time);
            System.out.println((cloudlet.getCloudletId() + 1) + " fastest run time : " + fastest_run_time);
            fastest_run_times.put(cloudlet, fastest_run_time);
            total_run_time += fastest_run_time;
        }
        long extra = criticalPath.getTotalRuntime() - total_run_time;
        long additional_time = extra / criticalPath.getPath().size();
        double k = 1.0 * criticalPath.getTotalRuntime() / total_run_time;
        System.out.println("totall new critical time:" + total_run_time);
        System.out.println("--------------");
        Collections.reverse(criticalPath.getPath());


        for (int i = 0; i < criticalPath.getPath().size(); i++)
        {
            Task criticalTask = criticalPath.getPath().get(i);
            Cloudlet cloudlet = criticalPath.getPath().get(i);
            Task parent = criticalTask.getCriticalParent(getCloudletList());
            if (parent == null)
            {
                long val = (long) (additional_time + fastest_run_times.get(cloudlet));
//                long val = (long) (k * fastest_run_times.get(cloudlet));
                subDeadLines.put(cloudlet, val);
            }
            else
            {
                long parent_val = subDeadLines.get(parent);
                long val = (long) (additional_time + fastest_run_times.get(cloudlet));
//                long val = (long) (k * fastest_run_times.get(cloudlet));
                subDeadLines.put(cloudlet, parent_val + val);
            }
        }
        System.out.printf("\n");
    }

    @Override
    protected void submitCloudlets() {
        if (criticalPaths == null)
        {
            remindTime = new HashMap<>();
            criticalPaths = findPCP(getCloudletList());
            subDeadLines = new HashMap<>();

            for (CriticalPath criticalPath : criticalPaths) {
                setSubDeadlines(criticalPath, criticalPath.getTotalRuntime());
            }

            System.out.println("##############################");
            for (Cloudlet cloudlet : subDeadLines.keySet())
            {
                System.out.println((cloudlet.getCloudletId() + 1) + " average run time:" + cloudlet.getCloudletLength() +
                        ", deadline :" + subDeadLines.get(cloudlet));
            }
            System.out.println("##############################");
        }
        List<Task> ReadyTasks = new ArrayList<>();
        for (Cloudlet cloudlet : getCloudletList()) {
            if (checkReady((Task) cloudlet)) {
                ReadyTasks.add((Task) cloudlet);
            }
        }


        Collections.sort(ReadyTasks, (t1, t2) ->
					Integer.compare(getCriticalPathIndex(getCriticalPath(t1))
                            , getCriticalPathIndex(getCriticalPath(t2))));


        System.out.println("================================");
        for (Cloudlet cloudlet : ReadyTasks) {
            System.out.println("we want to find vm for " + (1 + cloudlet.getCloudletId()));
            Vm vm = null;
            CriticalPath criticalPath = getCriticalPath(cloudlet);
            for (Vm checkVM : getVmsCreatedList())
            {
                System.out.print("now check vm : " + checkVM.getId() + ": ");
                if (canExecutedInVM(cloudlet, checkVM, subDeadLines.get(cloudlet)))
                {
                    System.out.println("passed :)");
                    vm = checkVM;
                    cloudlet.setVmId(vm.getId());
                    sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                    cloudletsSubmitted++;
                    getCloudletSubmittedList().add(cloudlet);
                    break;
                }
                else {
                    System.out.println("Reject!");
                }
            }

            if (vm == null)
            {
                System.out.println("Can not find a suitable VM to run" + cloudlet.getCloudletId());
                System.exit(-1);
            }
        }
        System.out.println("================================");
        // remove submitted cloudlets from waiting list
        for (Cloudlet cloudlet : getCloudletSubmittedList()) {
            getCloudletList().remove(cloudlet);
        }

    }

    protected CriticalPath getCriticalPath(Cloudlet cloudlet)
    {
        if (criticalPaths == null) return null;
        for (CriticalPath criticalPath : criticalPaths)
        {
            if (criticalPath.getPath().contains(cloudlet)) return criticalPath;
        }
        return null;
    }

    protected int getCriticalPathIndex(CriticalPath criticalPath)
    {
        for (int i = 0; i < criticalPaths.size(); i++)
        {
            if (criticalPath == criticalPaths.get(i)) return i;
        }
        return -1;
    }

    protected boolean canExecutedInVM(Cloudlet cloudlet, Vm vm, long deadLine)
    {
//        check vm is busy or not
        if (vm.getCloudletScheduler().runningCloudlets() > 0) return false;
//        check vm can run task before deadline or not
        long runtime = (long) (getAverageMips(cloudlet) / vm.getMips() * cloudlet.getCloudletLength());
        System.out.println("========={runtime in vm: " + runtime + ", deadline is :" + deadLine + "}");
        long cl = (long) (CloudSim.clock() * 1000);
        System.out.println("<clock> : " + cl + " (clock+runtime):" + (cl + runtime));
        if (cl + runtime > deadLine) return  false;
        return true;
    }

    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);


        CriticalPath criticalPath = getCriticalPath(cloudlet);
        long cpu = (long) cloudlet.getActualCPUTime() * 1000;
        long deadline = subDeadLines.get(cloudlet);
        long remind = deadline - cpu;

        System.out.println("@clout id:" + cloudlet.getCloudletId() + " cpu" + cpu + " deadline" + deadline + " cpu-deadline" + remind);
        remindTime.replace(criticalPath, remind);

        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
                + " received");
        cloudletsSubmitted--;
        if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
            Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
            clearDatacenters();
            finishExecution();
        } else { // some cloudlets haven't finished yet
            if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
                // all the cloudlets sent finished. It means that some bount
                // cloudlet is waiting its VM be created
                submitCloudlets();
//				clearDatacenters();
//				createVmsInDatacenter(0);
            }

        }
    }

    @Override
    public void shutdownEntity() {
        // TODO Auto-generated method stub
        super.shutdownEntity();
    }


    public void printCriticalParent(List<Task> tasks) {
        for (Task task : tasks) {
            if (task.getCriticalParent(tasks) != null) {
                Log.printLine("Task id = " + task.getCloudletId() + "====> critical parent time = "
                        + task.getCriticalRuntime() + ", parent id = " + task.getCriticalParent(tasks).getCloudletId());
            } else {
                Log.printLine("Task id = " + task.getCloudletId() + "====> critical parent time = "
                        + task.getCriticalRuntime());
            }
        }
    }

    /*
     * find PCP -> find every partial critical path and save it in list, all PCPs are available in list of
     * CriticalPath (which contains one partial critical path)
     */

    public List<CriticalPath> findPCP(List<Task> tasks){
        List<Task> copy = new ArrayList<>();
        copy.addAll(tasks);
        List<CriticalPath> pcp = new ArrayList<>();
        Collections.sort(copy, (t1, t2) ->
                Integer.compare(t2.getCloudletId(), t1.getCloudletId()));
//        System.out.println("#########################");
//        System.out.println(copy.size());
//        System.out.println("The list of task in copy : [0]"+ copy);
        for (int i = 0; i < copy.size(); i++){
//            System.out.println("now in "+ copy.get(i).getCloudletId());
            CriticalPath cp = findCriticalPath(copy, copy.get(i));
            pcp.add(cp);
            copy.removeAll(cp.getPath());
            if (cp.getPath().size() > 1)
            {
                i--;
            }
//            System.out.println("The list of task in copy : [" + (i + 1) +"]"+ copy);
        }
//        System.out.println("#########################");
//        System.out.println(pcp.size());
        printPCP(pcp);

        return pcp;
    }

    /*
     * find a partial critical path -> choose critical parent (that not in critical path already)
     * and add it to a list. The list includes of critical parent of ... critical parent of node.
     */

    public CriticalPath findCriticalPath(List<Task> tasks,Task task){
        Task temp = task;
        List<Task> path = new ArrayList<>();
        path.add(task);
        while(temp.getCriticalParent(tasks) != null){
            path.add(temp.getCriticalParent(tasks));
            temp = temp.getCriticalParent(tasks);
        }
        return new CriticalPath(path);
    }

    public void printPCP(List<CriticalPath> pcp){
        for (CriticalPath cp : pcp){
            System.out.println("critical path total runtime = " + cp.getTotalRuntime());
            for (Task task : cp.getPath()){
                System.out.println("task id = " + task.getCloudletId() + ", task runtime = " + task.getCloudletLength());
            }
        }
    }

}