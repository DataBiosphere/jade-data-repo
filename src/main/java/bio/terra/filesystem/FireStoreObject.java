package bio.terra.filesystem;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * POJO for writing file system object entries to FireStore. This object is used
 * for both directories and file references. Fields not used for one or the other
 * are left as null.
 *
 * NOTE: an alternative would be to use the map version of object creation and leave
 * out fields. That makes the JAVA code more complex. So I decided to use POJOs to
 * model the objects as they are stored and convert them to and from FSObject.
 *
 * Requirements from the documentation are:
 *   "Each custom class must have a public constructor that takes no arguments.
 *    In addition, the class must include a public getter for each property."
 */

public class FireStoreObject {
    // common fields for dirs and filerefs
    private String objectId;
    private boolean fileRef; // true means it is a file reference; false means it is a directory
    private String path; // path to the object
    private String name; // name of the object

    // fileref-only fields
    private String datasetId;

    // directory-only fields
    private String fileCreatedDate;

    // snapshot directory-only fields - computed as part of snapshot creation
    private String checksumCrc32c;
    private String checksumMd5;
    private Long size;

    public FireStoreObject() {
    }

    public String getObjectId() {
        return objectId;
    }

    public FireStoreObject objectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public boolean getFileRef() {
        return fileRef;
    }

    public FireStoreObject fileRef(boolean fileRef) {
        this.fileRef = fileRef;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FireStoreObject path(String path) {
        this.path = path;
        return this;
    }

    public String getName() {
        return name;
    }

    public FireStoreObject name(String name) {
        this.name = name;
        return this;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public FireStoreObject datasetId(String datasetId) {
        this.datasetId = datasetId;
        return this;
    }

    public String getFileCreatedDate() {
        return fileCreatedDate;
    }

    public FireStoreObject fileCreatedDate(String fileCreatedDate) {
        this.fileCreatedDate = fileCreatedDate;
        return this;
    }

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FireStoreObject checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FireStoreObject checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public FireStoreObject size(Long size) {
        this.size = size;
        return this;
    }

    public FireStoreObject copyObjectUnderNewPath(String newPath) {
        String path = "/" + newPath + getPath();
        return new FireStoreObject()
            .objectId(getObjectId())
            .fileRef(getFileRef())
            .path(newPath)
            .name(getName())
            .datasetId(getDatasetId())
            .fileCreatedDate(getFileCreatedDate())
            .checksumCrc32c(getChecksumCrc32c())
            .checksumMd5(getChecksumMd5())
            .size(getSize());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("objectId", objectId)
            .append("path", path)
            .append("name", name)
            .append("datasetId", datasetId)
            .append("fileCreatedDate", fileCreatedDate)
            .append("checksumCrc32c", checksumCrc32c)
            .append("checksumMd5", checksumMd5)
            .append("size", size)
            .toString();
    }
}
