package PCP;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;
import utils.ReplicaCatalog;
import utils.Task;
import utils.WorkflowParser;
import utils.WorkflowType;

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
        int mips1 = 500;
        int mips2 = 1000;
        int mips3 = 1500;
        int mips4 = 2000;
        long bw = 1000;
        int nw = 500;
        int vms = (int) Math.ceil(nw / (14 * dw));

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
        submitVmList(vmList);

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
        for (Task parent : task.getParentList()) {
            for (Cloudlet cloudlet : getCloudletReceivedList()) {
                if (parent.getCloudletId() != cloudlet.getCloudletId()) {
                    return false;
                }
            }

        }
        return true;
    }

    @Override
    protected void submitCloudlets() {
        int vmIndex = 0;
        List<Task> ReadyTasks = new ArrayList<>();

        for (Cloudlet cloudlet : getCloudletList()) {
            if (checkReady((Task) cloudlet)) {
                ReadyTasks.add((Task) cloudlet);
            }
        }

        findPCP(ReadyTasks);

        for (Cloudlet cloudlet : ReadyTasks) {
            System.out.println("cloudlet: " + cloudlet.getCloudletId() + " , runtime: " + cloudlet.getCloudletLength());
        }

        for (Cloudlet cloudlet : ReadyTasks) {
            Vm vm;
            // if user didn't bind this cloudlet and it has not been executed yet
            if (cloudlet.getVmId() == -1) {
                vm = getVmsCreatedList().get(vmIndex);
            } else { // submit to the specific vm
                vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                if (vm == null) { // vm was not created
                    Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
                            + cloudlet.getCloudletId() + ": bount VM not available");
                    continue;
                }
            }

            Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
                    + cloudlet.getCloudletId() + " to VM #" + vm.getId());
            cloudlet.setVmId(vm.getId());
            sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            cloudletsSubmitted++;
            vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            getCloudletSubmittedList().add(cloudlet);
        }

        // remove submitted cloudlets from waiting list
        for (Cloudlet cloudlet : getCloudletSubmittedList()) {
            getCloudletList().remove(cloudlet);
        }
    }

    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);
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
        System.out.println("#########################");
        System.out.println(copy.size());
        System.out.println("The list of task in copy : [0]"+ copy);
        for (int i = 0; i < copy.size(); i++){
            System.out.println("now in "+ copy.get(i).getCloudletId());
            CriticalPath cp = findCriticalPath(copy, copy.get(i));
            pcp.add(cp);
            copy.removeAll(cp.getPath());
            if (cp.getPath().size() > 1)
            {
                i--;
            }
            System.out.println("The list of task in copy : [" + (i + 1) +"]"+ copy);
        }
        System.out.println("#########################");
        System.out.println(pcp.size());
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