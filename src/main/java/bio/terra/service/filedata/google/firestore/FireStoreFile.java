package bio.terra.service.filedata.google.firestore;

import bio.terra.model.FileModel;
import bio.terra.service.filedata.FileService;
import com.azure.data.tables.models.TableEntity;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * POJO for file objects held in our FireStore files collection. This object is used to describe
 * physical files of a dataset. There is one collection named: "<dataset-id>-files" and the file
 * objects are named with their object ids. All path naming is done in the directory collection.
 *
 * <p>Requirements from the documentation are: "Each custom class must have a public constructor
 * that takes no arguments. In addition, the class must include a public getter for each property."
 */
public class FireStoreFile {
  private String fileId;
  private String mimeType;
  private String description;
  private String bucketResourceId;
  private String loadTag;
  // fields filled in from FSFileInfo after the file ingest
  private String fileCreatedDate;
  private String gspath;
  private String checksumCrc32c;
  private String checksumMd5;
  private boolean userSpecifiedMd5;
  private Long size;

  // Azure table entity field names
  public static final String FILE_ID_FIELD_NAME = "fileId";
  public static final String MIME_TYPE_FIELD_NAME = "mimeType";
  public static final String DESCRIPTION_FIELD_NAME = "description";
  public static final String BUCKET_RESOURCE_ID_FIELD_NAME = "bucketResourceId";
  public static final String LOAD_TAG_FIELD_NAME = "loadTag";
  public static final String FILE_CREATED_DATE_FIELD_NAME = "fileCreatedDate";
  public static final String GS_PATH_FIELD_NAME = "gspath";
  public static final String PATH_FIELD_NAME = "path";
  public static final String CHECKSUM_CRC32C_FIELD_NAME = "checksum_crc32c";
  public static final String CHECKSUM_MD5_FIELD_NAME = "checksum_md5";
  public static final String USER_SPECIFIED_MD5_FIELD_NAME = "userSpecifiedMd5";
  public static final String SIZE_FIELD_NAME = "size";

  public FireStoreFile() {}

  public String getFileId() {
    return fileId;
  }

  public FireStoreFile fileId(String fileId) {
    this.fileId = fileId;
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

  public boolean isUserSpecifiedMd5() {
    return userSpecifiedMd5;
  }

  public FireStoreFile userSpecifiedMd5(boolean userSpecifiedMd5) {
    this.userSpecifiedMd5 = userSpecifiedMd5;
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

  public String getBucketResourceId() {
    return bucketResourceId;
  }

  public FireStoreFile bucketResourceId(String bucketResourceId) {
    this.bucketResourceId = bucketResourceId;
    return this;
  }

  public String getLoadTag() {
    return loadTag;
  }

  public FireStoreFile loadTag(String loadTag) {
    this.loadTag = loadTag;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("fileId", fileId)
        .append("mimeType", mimeType)
        .append("description", description)
        .append("bucketResourceId", bucketResourceId)
        .append("loadTag", loadTag)
        .append("fileCreatedDate", fileCreatedDate)
        .append("gspath", gspath)
        .append("checksumCrc32c", checksumCrc32c)
        .append("checksumMd5", checksumMd5)
        .append("userSpecifiedMd5", userSpecifiedMd5)
        .append("size", size)
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
    FireStoreFile that = (FireStoreFile) o;
    return Objects.equals(fileId, that.fileId)
        && Objects.equals(mimeType, that.mimeType)
        && Objects.equals(description, that.description)
        && Objects.equals(bucketResourceId, that.bucketResourceId)
        && Objects.equals(loadTag, that.loadTag)
        && Objects.equals(fileCreatedDate, that.fileCreatedDate)
        && Objects.equals(gspath, that.gspath)
        && Objects.equals(checksumCrc32c, that.checksumCrc32c)
        && Objects.equals(checksumMd5, that.checksumMd5)
        && Objects.equals(userSpecifiedMd5, that.userSpecifiedMd5)
        && Objects.equals(size, that.size);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fileId,
        mimeType,
        description,
        bucketResourceId,
        loadTag,
        fileCreatedDate,
        gspath,
        checksumCrc32c,
        checksumMd5,
        userSpecifiedMd5,
        size);
  }

  public static FireStoreFile fromTableEntity(TableEntity entity) {
    return new FireStoreFile()
        .fileId((String) entity.getProperty(FILE_ID_FIELD_NAME))
        .mimeType((String) entity.getProperty(MIME_TYPE_FIELD_NAME))
        .description((String) entity.getProperty(DESCRIPTION_FIELD_NAME))
        .bucketResourceId((String) entity.getProperty(BUCKET_RESOURCE_ID_FIELD_NAME))
        .loadTag((String) entity.getProperty(LOAD_TAG_FIELD_NAME))
        .fileCreatedDate((String) entity.getProperty(FILE_CREATED_DATE_FIELD_NAME))
        .gspath((String) entity.getProperty(GS_PATH_FIELD_NAME))
        .checksumCrc32c((String) entity.getProperty(CHECKSUM_CRC32C_FIELD_NAME))
        .checksumMd5((String) entity.getProperty(CHECKSUM_MD5_FIELD_NAME))
        .userSpecifiedMd5(
            (boolean)
                Objects.requireNonNullElse(
                    entity.getProperty(USER_SPECIFIED_MD5_FIELD_NAME), false))
        .size((Long) entity.getProperty(SIZE_FIELD_NAME));
  }

  public static TableEntity toTableEntity(String partitionKey, FireStoreFile f) {
    return new TableEntity(partitionKey, f.getFileId())
        .addProperty(FILE_ID_FIELD_NAME, f.getFileId())
        .addProperty(MIME_TYPE_FIELD_NAME, f.getMimeType())
        .addProperty(DESCRIPTION_FIELD_NAME, f.getDescription())
        .addProperty(BUCKET_RESOURCE_ID_FIELD_NAME, f.getBucketResourceId())
        .addProperty(LOAD_TAG_FIELD_NAME, f.getLoadTag())
        .addProperty(FILE_CREATED_DATE_FIELD_NAME, f.getFileCreatedDate())
        .addProperty(GS_PATH_FIELD_NAME, f.getGspath())
        .addProperty(CHECKSUM_CRC32C_FIELD_NAME, f.getChecksumCrc32c())
        .addProperty(CHECKSUM_MD5_FIELD_NAME, f.getChecksumMd5())
        .addProperty(USER_SPECIFIED_MD5_FIELD_NAME, f.isUserSpecifiedMd5())
        .addProperty(SIZE_FIELD_NAME, f.getSize());
  }

  public static FileModel toFileModel(FireStoreFile f, String collectionName, String datasetId) {
    return new FileModel()
        .fileId(f.getFileId())
        .collectionId(collectionName)
        .size(f.getSize())
        .created(f.getFileCreatedDate())
        .description(f.getDescription())
        .checksums(FileService.makeChecksums(f.getChecksumCrc32c(), f.getChecksumMd5()));
  }
}
