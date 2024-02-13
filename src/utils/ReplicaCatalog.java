package utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReplicaCatalog {

    /**
     * File System
     */
    public enum FileSystem {
        SHARED, LOCAL
    }
    /**
     * Map from file name to a file object
     */
    private static Map<String, FileItem> fileName2File;
    /**
     * The selection of file.system
     */
    private static FileSystem fileSystem;
    /**
     * Map from file to a list of data storage
     */
    private static Map<String, List<String>> dataReplicaCatalog;

    /**
     * Initialize a utils.ReplicaCatalog
     *
     * @param fs the type of file system
     */
    public static void init(FileSystem fs) {
        fileSystem = fs;
        dataReplicaCatalog = new HashMap<>();
        fileName2File = new HashMap<>();
    }

    /**
     * Gets the file system
     *
     * @return file system
     */
    public static FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Gets the file object based its file name
     *
     * @param fileName, file name
     * @return file object
     */
    public static FileItem getFile(String fileName) {
        return fileName2File.get(fileName);
    }

    /**
     * Adds a file name and the associated file object
     *
     * @param fileName, the file name
     * @param file , the file object
     */
    public static void setFile(String fileName, FileItem file) {
        fileName2File.put(fileName, file);
    }

    /**
     * Checks whether a file exists
     *
     * @param fileName file name
     * @return boolean, whether the file exist
     */
    public static boolean containsFile(String fileName) {
        return fileName2File.containsKey(fileName);
    }

    /**
     * Gets the list of storages a file exists
     *
     * @param file the file object
     * @return list of storages
     */
    public static List<String> getStorageList(String file) {
        return dataReplicaCatalog.get(file);
    }

    /**
     * Adds a file to a storage
     *
     * @param file, a file object
     * @param storage , the storage associated with this file
     */
    public static void addFileToStorage(String file, String storage) {
        if (!dataReplicaCatalog.containsKey(file)) {
            dataReplicaCatalog.put(file, new ArrayList<>());
        }
        List<String> list = getStorageList(file);
        if (!list.contains(storage)) {
            list.add(storage);
        }
    }
}
