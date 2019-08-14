package bio.terra.filesystem;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * POJO for file objects to FireStore. This object is only used for files and
 * stored in the "<dataset-id>-files" collection.
 *
 * Requirements from the documentation are:
 *   "Each custom class must have a public constructor that takes no arguments.
 *    In addition, the class must include a public getter for each property."
 */

public class FireStoreFile {
    private String objectId;
    private String objectStateLetter;
    private String fileCreatedDate;
    private String gspath;
    private String checksumCrc32c;
    private String checksumMd5;
    private Long size;              // 0 for directory
    private String mimeType;
    private String description;
    private String flightId;
    private String profileId;
    private String region;
    private String bucketResourceId;

    public FireStoreFile() {
    }

    public String getObjectId() {
        return objectId;
    }

    public FireStoreFile objectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public String getObjectStateLetter() {
        return objectStateLetter;
    }

    public FireStoreFile objectStateLetter(String objectStateLetter) {
        this.objectStateLetter = objectStateLetter;
        return this;
    }

    public String getFileCreatedDate() {
        return fileCreatedDate;
    }

    public FireStoreFile fileCreatedDate(String fileCreatedDate) {
        this.fileCreatedDate = fileCreatedDate;
        return this;
    }

    public String getGspath() {
        return gspath;
    }

    public FireStoreFile gspath(String gspath) {
        this.gspath = gspath;
        return this;
    }

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FireStoreFile checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FireStoreFile checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public FireStoreFile size(Long size) {
        this.size = size;
        return this;
    }

    public String getMimeType() {
        return mimeType;
    }

    public FireStoreFile mimeType(String mimeType) {
        this.mimeType = mimeType;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FireStoreFile description(String description) {
        this.description = description;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public FireStoreFile flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    public String getProfileId() {
        return profileId;
    }

    public FireStoreFile profileId(String profileId) {
        this.profileId = profileId;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public FireStoreFile region(String region) {
        this.region = region;
        return this;
    }

    public String getBucketResourceId() {
        return bucketResourceId;
    }

    public FireStoreFile bucketResourceId(String bucketResourceId) {
        this.bucketResourceId = bucketResourceId;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("objectId", objectId)
            .append("objectStateLetter", objectStateLetter)
            .append("fileCreatedDate", fileCreatedDate)
            .append("gspath", gspath)
            .append("checksumCrc32c", checksumCrc32c)
            .append("checksumMd5", checksumMd5)
            .append("size", size)
            .append("mimeType", mimeType)
            .append("description", description)
            .append("flightId", flightId)
            .append("profileId", profileId)
            .append("region", region)
            .append("bucketResourceId", bucketResourceId)
            .toString();
    }
}
