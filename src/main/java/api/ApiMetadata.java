package api;

public class ApiMetadata {
    public int key;
    public int minVersion;
    public int maxVersion;

    public ApiMetadata(int key, int minVersion, int maxVersion) {
        this.key = key;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }
}
