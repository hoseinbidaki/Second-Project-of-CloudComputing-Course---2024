package utils;

import org.cloudbus.cloudsim.Log;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class WorkflowParser {

    private final String daxPath;
    /**
     * All tasks.
     */
    private List<Task> taskList;
    /**
     * User id. used to create a new task.
     */
    private final int userId;

    /**
     * The scale of runtime. Multiple runtime by this
     */
    private static double runtime_scale = 1.0;

    /**
     * current job id. In case multiple workflow submission
     */
    private int jobIdStartsFrom;

    public List<Task> getTaskList() {
        return taskList;
    }

    protected void setTaskList(List<Task> taskList) {
        this.taskList = taskList;
    }

    /**
     * Map from task name to task.
     */
    protected Map<String, Task> mName2Task;

    public WorkflowParser(int userId, String daxPath) {
        this.userId = userId;
        this.mName2Task = new HashMap<>();
        this.daxPath = daxPath;
//        this.daxPaths = getDAXPaths();
        this.jobIdStartsFrom = 0;

        setTaskList(new ArrayList<>());
    }

    /**
     * Start to parse a workflow which is a xml file(s).
     */
    public void parse() {
        if (this.daxPath != null) {
            parseXmlFile(this.daxPath);
        } else {
            Log.print("file not found");
        }
    }

    private void parseXmlFile(String path) {
        try {
            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
            for (Element node : list) {
                switch (node.getName().toLowerCase()) {
                    case "job":
                        long length = 0;
                        String nodeName = node.getAttributeValue("id");
                        String nodeType = node.getAttributeValue("name");
                        /**
                         * capture runtime. If not exist, by default the runtime
                         * is 0.1. Otherwise CloudSim would ignore this task.
                         * BUG/#11
                         */
                        double runtime;
                        if (node.getAttributeValue("runtime") != null) {
                            String nodeTime = node.getAttributeValue("runtime");
                            runtime = 1000 * Double.parseDouble(nodeTime);
                            if (runtime < 100) {
                                runtime = 100;
                            }
                            length = (long) runtime;
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }   //multiple the scale, by default it is 1.0
                        length *= getRuntimeScale();
                        List<Element> fileList = node.getChildren();
                        List<FileItem> mFileList = new ArrayList<>();
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");//DAX version 3.3
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");//DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }

                                /**
                                 * a bug of cloudsim, size 0 causes a problem. 1
                                 * is ok.
                                 */
                                if (size == 0) {
                                    size++;
                                }
                                /**
                                 * Sets the file type 1 is input 2 is output
                                 */
                                FileType type = FileType.NONE;
                                switch (inout) {
                                    case "input":
                                        type = FileType.INPUT;
                                        break;
                                    case "output":
                                        type = FileType.OUTPUT;
                                        break;
                                    default:
                                        Log.printLine("Parsing Error");
                                        break;
                                }
                                FileItem tFile;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    /*
                                     * Assuming it is a parsing error
                                     */
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == FileType.OUTPUT) {
                                    /**
                                     * It is good that CloudSim does tell
                                     * whether a size is zero
                                     */
                                    tFile = new FileItem(fileName, size);
                                } else if (ReplicaCatalog.containsFile(fileName)) {
                                    tFile = ReplicaCatalog.getFile(fileName);
                                } else {
                                    tFile = new FileItem(fileName, size);
                                    ReplicaCatalog.setFile(fileName, tFile);
                                }

                                tFile.setType(type);
                                mFileList.add(tFile);

                            }
                        }
                        Task task;
                        //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
                        synchronized (this) {
                            task = new Task(this.jobIdStartsFrom, length);
                            this.jobIdStartsFrom++;
                        }
                        task.setType(nodeType);
                        task.setUserId(userId);
                        mName2Task.put(nodeName, task);
                        for (FileItem file : mFileList) {
                            task.addRequiredFile(file.getName());
                        }
                        task.setFileList(mFileList);
                        this.getTaskList().add(task);

                        /**
                         * Add dependencies info.
                         */
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (mName2Task.containsKey(childName)) {

                            Task childTask = (Task) mName2Task.get(childName);

                            for (Element parent : pList) {
                                String parentName = parent.getAttributeValue("ref");
                                if (mName2Task.containsKey(parentName)) {
                                    Task parentTask = (Task) mName2Task.get(parentName);
                                    parentTask.addChild(childTask);
                                    childTask.addParent(parentTask);
                                }
                            }
                        }
                        break;
                }
            }
            /**
             * If a task has no parent, then it is root task.
             */
            ArrayList roots = new ArrayList<>();
            for (Task task : mName2Task.values()) {
                task.setDepth(0);
                if (task.getParentList().isEmpty()) {
                    roots.add(task);
                }
            }

            /**
             * Add depth from top to bottom.
             */
            for (Iterator it = roots.iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                setDepth(task, 1);
            }
            /*
             * Clean them so as to save memory. Parsing workflow may take much
             * memory
             */
            this.mName2Task.clear();

        } catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");

        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
    }

    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    public static double getRuntimeScale(){
        return runtime_scale;
    }
}
