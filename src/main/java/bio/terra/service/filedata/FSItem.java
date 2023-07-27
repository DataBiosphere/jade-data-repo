package bio.terra.service.filedata;

import java.time.Instant;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This provides the base class for files and directories.
 *
 * <ul>
 *   <li>FSFile - describes a file
 *   <li>FSDir - describes a directory and, optionally, its contents
 * </ul>
 */
public class FSItem implements ChecksumInterface {
  private UUID fileId;
  private UUID collectionId;
  private Instant createdDate;
  private String path;
  private String checksumCrc32c;
  private String checksumMd5;
  private Long size;
  private String description;

  public FSItem() {}

  public UUID getFileId() {
    return fileId;
  }

  public FSItem fileId(UUID fileId) {
    this.fileId = fileId;
    return this;
  }

  public UUID getCollectionId() {
    return collectionId;
  }

  public FSItem collectionId(UUID datasetId) {
    this.collectionId = datasetId;
    return this;
  }

  public Instant getCreatedDate() {
    return createdDate;
  }

  public FSItem createdDate(Instant createdDate) {
    this.createdDate = createdDate;
    return this;
  }

  public String getPath() {
    return path;
  }

  public FSItem path(String path) {
    this.path = path;
    return this;
  }

  public String getChecksumCrc32c() {
    return checksumCrc32c;
  }

  public FSItem checksumCrc32c(String checksumCrc32c) {
    this.checksumCrc32c = checksumCrc32c;
    return this;
  }

  public String getChecksumMd5() {
    return checksumMd5;
  }

  public FSItem checksumMd5(String checksumMd5) {
    this.checksumMd5 = checksumMd5;
    return this;
  }

  public Long getSize() {
    return size;
  }

  public FSItem size(Long size) {
    this.size = size;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public FSItem description(String description) {
    this.description = description;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    FSItem that = (FSItem) o;

    return new EqualsBuilder()
        .append(fileId, that.fileId)
        .append(collectionId, that.collectionId)
        .append(createdDate, that.createdDate)
        .append(path, that.path)
        .append(checksumCrc32c, that.checksumCrc32c)
        .append(checksumMd5, that.checksumMd5)
        .append(size, that.size)
        .append(description, that.description)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(fileId)
        .append(collectionId)
        .append(createdDate)
        .append(path)
        .append(checksumCrc32c)
        .append(checksumMd5)
        .append(size)
        .append(description)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("fileId", fileId)
        .append("collectionId", collectionId)
        .append("createdDate", createdDate)
        .append("path", path)
        .append("checksumCrc32c", checksumCrc32c)
        .append("checksumMd5", checksumMd5)
        .append("size", size)
        .append("description", description)
        .toString();
  }
}
