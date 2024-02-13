package utils;

import java.util.List;

public class FileItem {

    private String name;

    private double size;

    private FileType type;

    public FileItem(String name, double size) {
        this.name = name;
        this.size = size;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public double getSize() {
        return this.size;
    }

    public FileType getType() {
        return this.type;
    }
    
    /**
     * If a input file has an output file it does not need stage-in For
     * workflows, we have a rule that a file is written once and read many
     * times, thus if a file is an output file it means it is generated within
     * this job and then used by another task within the same job (or other jobs
     * maybe) This is useful when we perform horizontal clustering
     * @param list
     * @return
     */
    public boolean isRealInputFile(List<FileItem> list) {
        if (this.getType() == FileType.INPUT)//input file
        {
            for (FileItem another : list) {
                if (another.getName().equals(this.getName())
                        /**
                         * if another file is output file
                         */
                        && another.getType() == FileType.OUTPUT) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
