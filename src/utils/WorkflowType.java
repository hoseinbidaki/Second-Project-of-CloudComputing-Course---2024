package utils;

public enum WorkflowType {
    MONTAGE(8), CYBER_SHAKE(3), SIPHT(5), EPIGENOMICS(7);
    public final int value;

    private WorkflowType(int dw) {
        this.value = dw;
    }
}
