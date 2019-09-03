package bio.terra.metadata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.time.Instant;
import java.util.UUID;

public class FSFile extends FSObjectBase {
    private UUID datasetId;
    private String gspath;
    private String mimeType;
    private String profileId;
    private String region;
    private String bucketResourceId;

    public UUID getDatasetId() {
        return datasetId;
    }

    public FSFile datasetId(UUID datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getGspath() {
        return gspath;
    }

    public FSFile gspath(String gspath) {
        this.gspath = gspath;
        return this;
    }

    public String getMimeType() {
        return mimeType;
    }

    public FSFile mimeType(String mimeType) {
        this.mimeType = mimeType;
        return this;
    }

    public String getProfileId() {
        return profileId;
    }

    public FSFile profileId(String profileId) {
        this.profileId = profileId;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public FSFile region(String region) {
        this.region = region;
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
    public FSFile objectId(UUID objectId) {
        super.objectId(objectId);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FSFile fsFile = (FSFile) o;

        return new EqualsBuilder()
            .appendSuper(super.equals(o))
            .append(gspath, fsFile.gspath)
            .append(mimeType, fsFile.mimeType)
            .append(profileId, fsFile.profileId)
            .append(region, fsFile.region)
            .append(bucketResourceId, fsFile.bucketResourceId)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .appendSuper(super.hashCode())
            .append(gspath)
            .append(mimeType)
            .append(profileId)
            .append(region)
            .append(bucketResourceId)
            .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("gspath", gspath)
            .append("mimeType", mimeType)
            .append("profileId", profileId)
            .append("region", region)
            .append("bucketResourceId", bucketResourceId)
            .toString();
    }
}
