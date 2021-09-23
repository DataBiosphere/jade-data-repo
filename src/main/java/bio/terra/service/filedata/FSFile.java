package bio.terra.service.filedata;

import bio.terra.model.CloudPlatform;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class FSFile extends FSItem {
  private UUID datasetId;
  private String cloudPath;
  private CloudPlatform cloudPlatform;
  private String mimeType;
  private String bucketResourceId;
  private String loadTag;

  public UUID getDatasetId() {
    return datasetId;
  }

  public FSFile datasetId(UUID datasetId) {
    this.datasetId = datasetId;
    return this;
  }

  public String getCloudPath() {
    return cloudPath;
  }

  public FSFile cloudPath(String cloudPath) {
    this.cloudPath = cloudPath;
    return this;
  }

  public CloudPlatform getCloudPlatform() {
    return cloudPlatform;
  }

  public FSFile cloudPlatform(CloudPlatform cloudPlatform) {
    this.cloudPlatform = cloudPlatform;
    return this;
  }

  public String getMimeType() {
    return mimeType;
  }

  public FSFile mimeType(String mimeType) {
    this.mimeType = mimeType;
    return this;
  }

  public String getBucketResourceId() {
    return bucketResourceId;
  }

  public FSFile bucketResourceId(String bucketResourceId) {
    this.bucketResourceId = bucketResourceId;
    return this;
  }

  // setters for super object, so fluent style works without ordering dependency
  public FSFile fileId(UUID fileId) {
    super.fileId(fileId);
    return this;
  }

  public FSFile collectionId(UUID collectionId) {
    super.collectionId(collectionId);
    return this;
  }

  public FSFile createdDate(Instant createdDate) {
    super.createdDate(createdDate);
    return this;
  }

  public FSFile path(String path) {
    super.path(path);
    return this;
  }

  public FSFile checksumCrc32c(String checksumCrc32c) {
    super.checksumCrc32c(checksumCrc32c);
    return this;
  }

  public FSFile checksumMd5(String checksumMd5) {
    super.checksumMd5(checksumMd5);
    return this;
  }

  public FSFile size(Long size) {
    super.size(size); // Super size it!
    return this;
  }

  public FSFile description(String description) {
    super.description(description);
    return this;
  }

  public String getLoadTag() {
    return loadTag;
  }

  public FSFile loadTag(String loadTag) {
    this.loadTag = loadTag;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    FSFile fsFile = (FSFile) o;

    return new EqualsBuilder()
        .appendSuper(super.equals(o))
        .append(datasetId, fsFile.datasetId)
        .append(cloudPath, fsFile.cloudPath)
        .append(mimeType, fsFile.mimeType)
        .append(bucketResourceId, fsFile.bucketResourceId)
        .append(loadTag, fsFile.loadTag)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .appendSuper(super.hashCode())
        .append(datasetId)
        .append(cloudPath)
        .append(mimeType)
        .append(bucketResourceId)
        .append(loadTag)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("datasetId", datasetId)
        .append("cloudPath", cloudPath)
        .append("mimeType", mimeType)
        .append("bucketResourceId", bucketResourceId)
        .append("loadTag", loadTag)
        .toString();
  }
}
