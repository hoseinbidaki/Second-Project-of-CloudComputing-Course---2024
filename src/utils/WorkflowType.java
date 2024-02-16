package utils;

public enum WorkflowType {
    MONTAGE(8), CYBER_SHAKE(4), SIPHT(5), EPIGENOMICS(7),
    LIGO(5), TEST(5);
    public final int value;

    private WorkflowType(int dw) {
        this.value = dw;
    }
}
