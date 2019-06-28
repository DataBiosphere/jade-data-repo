package bio.terra.filesystem;

// POJO for storing dependency documents in FireStore
// Each dependency document maps a data snapshot to a study/file-in-study.
// It has a reference count to track the number of times the file has been used in the data snapshot.
public class FireStoreDependency {
    private String datasetId;
    private String studyId;
    private String objectId;
    private Long refCount;

    public FireStoreDependency() {
    }

    public String getDatasetId() {
        return datasetId;
    }

    public FireStoreDependency datasetId(String datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public FireStoreDependency studyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getObjectId() {
        return objectId;
    }

    public FireStoreDependency objectId(String objectId) {
        this.objectId = objectId;
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
