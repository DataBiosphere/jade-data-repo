package bio.terra.metadata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.time.Instant;
import java.util.UUID;

/**
 * This provides the base class for all file system objects.
 * <ul>
 *     <li>FSFile - describes a file</li>
 *     <li>FSDir - describes a directory and, optionally, its contents</li>
 * </ul>
 *
 */
public class FSObjectBase {
    private UUID objectId;
    private UUID collectionId;
    private Instant createdDate;
    private String path;
    private String checksumCrc32c;
    private String checksumMd5;
    private Long size;
    private String description;

    public FSObjectBase() {
    }

    public UUID getObjectId() {
        return objectId;
    }

    public FSObjectBase objectId(UUID objectId) {
        this.objectId = objectId;
        return this;
    }

    public UUID getCollectionId() {
        return collectionId;
    }

    public FSObjectBase collectionId(UUID datasetId) {
        this.collectionId = datasetId;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public FSObjectBase createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FSObjectBase path(String path) {
        this.path = path;
        return this;
    }

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FSObjectBase checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FSObjectBase checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public FSObjectBase size(Long size) {
        this.size = size;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FSObjectBase description(String description) {
        this.description = description;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FSObjectBase that = (FSObjectBase) o;

        return new EqualsBuilder()
            .append(objectId, that.objectId)
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
            .append(objectId)
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
            .append("objectId", objectId)
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
