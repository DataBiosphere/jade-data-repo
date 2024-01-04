package bio.terra.service.filedata;

import java.time.Instant;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

// POJO of the collected information of a file returned from the bucket to be stored in the
// filesystem.
public class FSFileInfo {
  private String fileId;
  private String createdDate;
  private String cloudPath;
  private String checksumCrc32c;
  private String checksumMd5;
  private boolean userSpecifiedMd5;
  private Long size;
  private String bucketResourceId;

  public String getFileId() {
    return fileId;
  }

  public FSFileInfo fileId(String fileId) {
    this.fileId = fileId;
    return this;
  }

  public String getCreatedDate() {
    return createdDate;
  }

  public FSFileInfo createdDate(String createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  public String getCloudPath() {
    return cloudPath;
  }

  public FSFileInfo cloudPath(String cloudPath) {
    this.cloudPath = cloudPath;
    return this;
  }

  public String getChecksumCrc32c() {
    return checksumCrc32c;
  }

  public FSFileInfo checksumCrc32c(String checksumCrc32c) {
    this.checksumCrc32c = checksumCrc32c;
    return this;
  }

  public String getChecksumMd5() {
    return checksumMd5;
  }

  public FSFileInfo checksumMd5(String checksumMd5) {
    this.checksumMd5 = checksumMd5;
    return this;
  }

  public boolean isUserSpecifiedMd5() {
    return userSpecifiedMd5;
  }

  public FSFileInfo userSpecifiedMd5(boolean userSpecifiedMd5) {
    this.userSpecifiedMd5 = userSpecifiedMd5;
    return this;
  }

  public Long getSize() {
    return size;
  }

  public FSFileInfo size(Long size) {
    this.size = size;
    return this;
  }

  public String getBucketResourceId() {
    return bucketResourceId;
  }

  public FSFileInfo bucketResourceId(String bucketResourceId) {
    this.bucketResourceId = bucketResourceId;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("fileId", fileId)
        .append("cloudPath", cloudPath)
        .append("checksumCrc32c", checksumCrc32c)
        .append("checksumMd5", checksumMd5)
        .append("userSpecifiedMd5", userSpecifiedMd5)
        .append("size", size)
        .append("bucketResourceId", bucketResourceId)
        .toString();
  }

  public static FSFileInfo getTestInstance(String fileId, String resourceId) {
    return new FSFileInfo()
        .fileId(fileId)
        .bucketResourceId(resourceId)
        .checksumCrc32c(null)
        .checksumMd5("baaaaaad")
        .userSpecifiedMd5(false)
        .createdDate(Instant.now().toString())
        .cloudPath("https://path")
        .size(100L);
  }
}
