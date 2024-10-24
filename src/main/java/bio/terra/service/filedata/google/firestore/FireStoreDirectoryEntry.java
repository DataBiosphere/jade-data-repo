package bio.terra.service.filedata.google.firestore;

import com.azure.data.tables.models.TableEntity;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * POJO for writing file system directory entries to FireStore. This object is used for both
 * directories and file references. Fields not used for one or the other are left as null. This is
 * not ideal from an OO point of view, but it works best with FireStore.
 *
 * <p>Requirements from the FireStore documentation are: "Each custom class must have a public
 * constructor that takes no arguments. In addition, the class must include a public getter for each
 * property."
 */
public class FireStoreDirectoryEntry {
  // common fields for dirs and filerefs
  private String fileId;
  private boolean isFileRef; // true means it is a file reference; false means it is a directory
  private String path; // path to the object
  private String name; // name of the object

  // fileref-only fields
  // FireStoreFile object in the files collection
  private String
      datasetId; // The pair (datasetId, fileId) are used to lookup the FireStoreFile in file
  // collection.
  private String loadTag; // load tag of the load that made this entry

  // directory-only fields
  private String
      fileCreatedDate; // For files, we get the created date from the FireStoreFile object

  // snapshot directory-only fields - computed as part of snapshot filesystem creation;
  // unused in the dataset directory collection
  private String checksumCrc32c;
  private String checksumMd5;
  private Long size;

  // Azure table entity field names
  public static final String FILE_ID_FIELD_NAME = "fileId";
  public static final String IS_FILE_REF_FIELD_NAME = "isFileRef";
  public static final String PATH_FIELD_NAME = "path";
  public static final String NAME_FIELD_NAME = "name";
  public static final String DATASET_ID_FIELD_NAME = "datasetId";
  public static final String FILE_CREATED_DATE_FIELD_NAME = "fileCreatedDate";
  public static final String CHECKSUM_CRC32C_FIELD_NAME = "checksum_crc32c";
  public static final String CHECKSUM_MD5_FIELD_NAME = "checksum_md5";
  public static final String SIZE_FIELD_NAME = "size";
  public static final String LOAD_TAG_FIELD_NAME = "loadTag";

  public FireStoreDirectoryEntry() {}

  public String getFileId() {
    return fileId;
  }

  public FireStoreDirectoryEntry fileId(String fileId) {
    this.fileId = fileId;
    return this;
  }

  public boolean getIsFileRef() {
    return isFileRef;
  }

  public FireStoreDirectoryEntry isFileRef(boolean fileRef) {
    isFileRef = fileRef;
    return this;
  }

  public String getPath() {
    return path;
  }

  public FireStoreDirectoryEntry path(String path) {
    this.path = path;
    return this;
  }

  public String getName() {
    return name;
  }

  public FireStoreDirectoryEntry name(String name) {
    this.name = name;
    return this;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public FireStoreDirectoryEntry datasetId(String datasetId) {
    this.datasetId = datasetId;
    return this;
  }

  public String getLoadTag() {
    return loadTag;
  }

  public FireStoreDirectoryEntry loadTag(String loadTag) {
    this.loadTag = loadTag;
    return this;
  }

  public String getFileCreatedDate() {
    return fileCreatedDate;
  }

  public FireStoreDirectoryEntry fileCreatedDate(String fileCreatedDate) {
    this.fileCreatedDate = fileCreatedDate;
    return this;
  }

  public String getChecksumCrc32c() {
    return checksumCrc32c;
  }

  public FireStoreDirectoryEntry checksumCrc32c(String checksumCrc32c) {
    this.checksumCrc32c = checksumCrc32c;
    return this;
  }

  public String getChecksumMd5() {
    return checksumMd5;
  }

  public FireStoreDirectoryEntry checksumMd5(String checksumMd5) {
    this.checksumMd5 = checksumMd5;
    return this;
  }

  public Long getSize() {
    return size;
  }

  public FireStoreDirectoryEntry size(Long size) {
    this.size = size;
    return this;
  }

  public FireStoreDirectoryEntry copyEntryUnderNewPath(String newPath) {
    String fullPath = StringUtils.removeEnd("/" + newPath + getPath(), "/");
    return new FireStoreDirectoryEntry()
        .fileId(getFileId())
        .isFileRef(getIsFileRef())
        .path(fullPath)
        .name(getName())
        .datasetId(getDatasetId())
        .fileCreatedDate(getFileCreatedDate())
        .checksumCrc32c(getChecksumCrc32c())
        .checksumMd5(getChecksumMd5())
        .size(getSize())
        .loadTag(getLoadTag());
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("fileId", fileId)
        .append("isFileRef", isFileRef)
        .append("path", path)
        .append("name", name)
        .append("datasetId", datasetId)
        .append("fileCreatedDate", fileCreatedDate)
        .append("checksumCrc32c", checksumCrc32c)
        .append("checksumMd5", checksumMd5)
        .append("size", size)
        .append("loadTag", loadTag)
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
    FireStoreDirectoryEntry that = (FireStoreDirectoryEntry) o;
    return Objects.equals(fileId, that.fileId)
        && Objects.equals(isFileRef, that.isFileRef)
        && Objects.equals(path, that.path)
        && Objects.equals(name, that.name)
        && Objects.equals(datasetId, that.datasetId)
        && Objects.equals(fileCreatedDate, that.fileCreatedDate)
        && Objects.equals(checksumCrc32c, that.checksumCrc32c)
        && Objects.equals(checksumMd5, that.checksumMd5)
        && Objects.equals(size, that.size)
        && Objects.equals(loadTag, that.loadTag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileId,
        isFileRef,
        path,
        name,
        datasetId,
        fileCreatedDate,
        checksumCrc32c,
        checksumMd5,
        size,
        loadTag);
  }

  public static FireStoreDirectoryEntry fromTableEntity(TableEntity entity) {
    return new FireStoreDirectoryEntry()
        .fileId(entity.getProperty(FILE_ID_FIELD_NAME).toString())
        .isFileRef((Boolean) entity.getProperty(IS_FILE_REF_FIELD_NAME))
        .path(entity.getProperty(PATH_FIELD_NAME).toString())
        .name(entity.getProperty(NAME_FIELD_NAME).toString())
        .datasetId((String) entity.getProperty(DATASET_ID_FIELD_NAME))
        .fileCreatedDate((String) entity.getProperty(FILE_CREATED_DATE_FIELD_NAME))
        .checksumCrc32c((String) entity.getProperty(CHECKSUM_CRC32C_FIELD_NAME))
        .checksumMd5((String) entity.getProperty(CHECKSUM_MD5_FIELD_NAME))
        .size((Long) entity.getProperty(SIZE_FIELD_NAME))
        .loadTag((String) entity.getProperty(LOAD_TAG_FIELD_NAME));
  }

  public static TableEntity toTableEntity(
      String partitionKey, String rowKey, FireStoreDirectoryEntry f) {
    return new TableEntity(partitionKey, rowKey)
        .addProperty(FILE_ID_FIELD_NAME, f.getFileId())
        .addProperty(IS_FILE_REF_FIELD_NAME, f.getIsFileRef())
        .addProperty(PATH_FIELD_NAME, f.getPath())
        .addProperty(NAME_FIELD_NAME, f.getName())
        .addProperty(DATASET_ID_FIELD_NAME, f.getDatasetId())
        .addProperty(FILE_CREATED_DATE_FIELD_NAME, f.getFileCreatedDate())
        .addProperty(CHECKSUM_CRC32C_FIELD_NAME, f.getChecksumCrc32c())
        .addProperty(CHECKSUM_MD5_FIELD_NAME, f.getChecksumMd5())
        .addProperty(SIZE_FIELD_NAME, f.getSize())
        .addProperty(LOAD_TAG_FIELD_NAME, f.getLoadTag());
  }
}
