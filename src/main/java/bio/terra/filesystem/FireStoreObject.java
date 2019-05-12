package bio.terra.filesystem;

import org.apache.commons.lang3.StringUtils;

/**
 * POJO for writing file system object entries to FireStore. This object is used
 * for both directories and files. Fields not used for directories are left as null.
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
    // common fields
    private String objectId;
    private String studyId;
    private String objectTypeLetter;
    private String path; // path to the object
    private String name; // name of the object
    // file-only fields
    private String fileCreatedDate;
    private String gspath;
    private String checksumCrc32c;
    private String checksumMd5;
    private Long size;              // 0 for directory
    private String mimeType;
    private String description;
    private String flightId;

    public FireStoreObject() {
    }

    public String getObjectId() {
        return objectId;
    }

    public FireStoreObject objectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public FireStoreObject studyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getObjectTypeLetter() {
        return objectTypeLetter;
    }

    public FireStoreObject objectTypeLetter(String objectTypeLetter) {
        this.objectTypeLetter = objectTypeLetter;
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

    public String getFileCreatedDate() {
        return fileCreatedDate;
    }

    public FireStoreObject fileCreatedDate(String fileCreatedDate) {
        this.fileCreatedDate = fileCreatedDate;
        return this;
    }

    public String getGspath() {
        return gspath;
    }

    public FireStoreObject gspath(String gspath) {
        this.gspath = gspath;
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

    public String getMimeType() {
        return mimeType;
    }

    public FireStoreObject mimeType(String mimeType) {
        this.mimeType = mimeType;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FireStoreObject description(String description) {
        this.description = description;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public FireStoreObject flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    public String getFullPath() {
        return getPath() + '/' + getName();
    }

    private static final char DOCNAME_SEPARATOR = '\u001c';
    public String getDocumentName() {
        return StringUtils.replaceChars(getFullPath(), '/', DOCNAME_SEPARATOR);
    }


}
