package bio.terra.metadata;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.time.Instant;
import java.util.UUID;

/**
 * This provides the base class for all file system objects. There are three specializations:
 * <ul>
 *     <li>FSFile - describes a file</li>
 *     <li>FSDir - describes a directory</li>
 *     <li>FSEnumDir - describes a directory and its contents</li>
 * </ul>
 *
 */
public class FSObjectBase {
    private UUID objectId;
    private UUID studyId;
    private FSObjectType objectType;
    private Instant createdDate;
    private String path;
    private Long size;              // 0 for directory
    private String description;

    // copy constructor
    public FSObjectBase(FSObjectBase other) {
        this.objectId = other.objectId;
        this.studyId = other.studyId;
        this.objectType = other.objectType;
        this.createdDate = other.createdDate;
        this.path = other.path;
        this.size = other.size;
        this.description = other.description;
    }

    public UUID getObjectId() {
        return objectId;
    }

    public FSObjectBase objectId(UUID objectId) {
        this.objectId = objectId;
        return this;
    }

    public UUID getStudyId() {
        return studyId;
    }

    public FSObjectBase studyId(UUID studyId) {
        this.studyId = studyId;
        return this;
    }

    public FSObjectType getObjectType() {
        return objectType;
    }

    public FSObjectBase objectType(FSObjectType objectType) {
        this.objectType = objectType;
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
    public String toString() {
        return new ToStringBuilder(this)
            .append("objectId", objectId)
            .append("studyId", studyId)
            .append("objectType", objectType)
            .append("createdDate", createdDate)
            .append("path", path)
            .append("size", size)
            .append("description", description)
            .toString();
    }
}
