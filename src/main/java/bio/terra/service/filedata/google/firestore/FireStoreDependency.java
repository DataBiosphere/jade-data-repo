package bio.terra.service.filedata.google.firestore;

import com.azure.data.tables.models.TableEntity;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

// POJO for storing dependency documents in FireStore
// Each dependency document maps a snapshot to a dataset/file-in-dataset.
// It has a reference count to track the number of times the file has been used in the snapshot.
public class FireStoreDependency {
  private String snapshotId;
  private String fileId;
  private Long refCount;

  // Azure table entity field names
  public static final String SNAPSHOT_ID_FIELD_NAME = "snapshotId";
  public static final String FILE_ID_FIELD_NAME = "fileId";
  public static final String REF_COUNT_FIELD_NAME = "refCount";

  public FireStoreDependency() {}

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

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("snapshotId", snapshotId)
        .append("fileId", fileId)
        .append("refCount", refCount)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FireStoreDependency that = (FireStoreDependency) o;
    return Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(fileId, that.fileId)
        && Objects.equals(refCount, that.refCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, fileId, refCount);
  }

  public static FireStoreDependency fromTableEntity(TableEntity entity) {
    return new FireStoreDependency()
        .snapshotId(entity.getProperty(SNAPSHOT_ID_FIELD_NAME).toString())
        .fileId(entity.getProperty(FILE_ID_FIELD_NAME).toString())
        .refCount((Long) entity.getProperty(REF_COUNT_FIELD_NAME));
  }

  public static TableEntity toTableEntity(String partitionKey, FireStoreDependency f) {
    return new TableEntity(partitionKey, f.getFileId())
        .addProperty(SNAPSHOT_ID_FIELD_NAME, f.getSnapshotId())
        .addProperty(FILE_ID_FIELD_NAME, f.getFileId())
        .addProperty(REF_COUNT_FIELD_NAME, f.getRefCount());
  }
}
