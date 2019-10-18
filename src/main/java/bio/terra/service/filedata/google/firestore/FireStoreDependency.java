package bio.terra.service.filedata.google.firestore;

// POJO for storing dependency documents in FireStore
// Each dependency document maps a snapshot to a dataset/file-in-dataset.
// It has a reference count to track the number of times the file has been used in the snapshot.
public class FireStoreDependency {
    private String snapshotId;
    private String fileId;
    private Long refCount;

    public FireStoreDependency() {
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public FireStoreDependency snapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public String getFileId() {
        return fileId;
    }

    public FireStoreDependency fileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public Long getRefCount() {
        return refCount;
    }

    public FireStoreDependency refCount(Long refCount) {
        this.refCount = refCount;
        return this;
    }
}
