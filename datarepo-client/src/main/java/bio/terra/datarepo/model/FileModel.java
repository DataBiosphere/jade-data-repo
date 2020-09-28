/*
 * Data Repository API
 * This document defines the REST API for Data Repository. **Status: design in progress**  There are four top-level endpoints (besides some used by swagger):  * /swagger-ui.html - generated by swagger: swagger API page that provides this documentation and a live UI for      submitting REST requests  * /status - provides the operational status of the service  * /api    - is the authenticated and authorized Data Repository API  * /ga4gh/drs/v1 - is a transcription of the Data Repository Service API  The overall API (/api) currently supports two interfaces:  * Repository - a general and default interface for initial setup, managing ingest and repository metadata  * Resource - an interface for managing billing accounts and resources  The API endpoints are organized by interface. Each interface is separately versioned.  ## Notes on Naming All of the reference items are suffixed with \"Model\". Those names are used as the class names in the generated Java code. It is helpful to distinguish these model classes from other related classes, like the DAO classes and the operation classes.  ## Editing and debugging I have found it best to edit this file directly to make changes and then use the swagger-editor to validate. The errors out of swagger-codegen are not that helpful. In the swagger-editor, it gives you nice errors and links to the place in the YAML where the errors are.  But... the swagger-editor has been a bit of a pain for me to run. I tried the online website and was not able to load my YAML. Instead, I run it locally in a docker container, like this: ``` docker pull swaggerapi/swagger-editor docker run -p 9090:8080 swaggerapi/swagger-editor ``` Then navigate to localhost:9090 in your browser.  I have not been able to get the file upload to work. It is a bit of a PITA, but I copy-paste the source code, replacing what is in the editor. Then make any fixes. Then copy-paste the resulting, valid file back into our source code. Not elegant, but easier than playing detective with the swagger-codegen errors.  This might be something about my browser or environment, so give it a try yourself and see how it goes.  ## Merging the DRS standard swagger into this swagger ##  The merging is done in three sections:  1. Merging the security definitions into our security definitions  2. This section of paths. We make all paths explicit (prefixed with /ga4gh/drs/v1)     All standard DRS definitions and parameters are prefixed with 'DRS' to separate them     from our native definitions and parameters. We remove the x-swagger-router-controller lines.  3. A separate part of the definitions section for the DRS definitions  NOTE: the code here does not relect the DRS spec anymore. See DR-409. 
 *
 * OpenAPI spec version: 0.1.0
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package bio.terra.datarepo.model;

import java.util.Objects;
import java.util.Arrays;
import bio.terra.datarepo.model.DRSChecksum;
import bio.terra.datarepo.model.DirectoryDetailModel;
import bio.terra.datarepo.model.FileDetailModel;
import bio.terra.datarepo.model.FileModelType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * A file or directory in the data repository
 */
@ApiModel(description = "A file or directory in the data repository")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class FileModel {
  @JsonProperty("fileId")
  private String fileId = null;

  @JsonProperty("collectionId")
  private String collectionId = null;

  @JsonProperty("path")
  private String path = null;

  @JsonProperty("size")
  private Long size = null;

  @JsonProperty("checksums")
  private List<DRSChecksum> checksums = null;

  @JsonProperty("created")
  private String created = null;

  @JsonProperty("description")
  private String description = null;

  @JsonProperty("fileType")
  private FileModelType fileType = null;

  @JsonProperty("fileDetail")
  private FileDetailModel fileDetail = null;

  @JsonProperty("directoryDetail")
  private DirectoryDetailModel directoryDetail = null;

  public FileModel fileId(String fileId) {
    this.fileId = fileId;
    return this;
  }

   /**
   * Unique id of the filesystem object within the dataset
   * @return fileId
  **/
  @ApiModelProperty(value = "Unique id of the filesystem object within the dataset")
  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public FileModel collectionId(String collectionId) {
    this.collectionId = collectionId;
    return this;
  }

   /**
   * Id of the dataset or snapshot directory describing the object
   * @return collectionId
  **/
  @ApiModelProperty(value = "Id of the dataset or snapshot directory describing the object")
  public String getCollectionId() {
    return collectionId;
  }

  public void setCollectionId(String collectionId) {
    this.collectionId = collectionId;
  }

  public FileModel path(String path) {
    this.path = path;
    return this;
  }

   /**
   * full path of the file in the dataset
   * @return path
  **/
  @ApiModelProperty(value = "full path of the file in the dataset")
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public FileModel size(Long size) {
    this.size = size;
    return this;
  }

   /**
   * Always present for files - the file size in bytes Present for directories in snapshots - sum of sizes of objects in a directory 
   * @return size
  **/
  @ApiModelProperty(value = "Always present for files - the file size in bytes Present for directories in snapshots - sum of sizes of objects in a directory ")
  public Long getSize() {
    return size;
  }

  public void setSize(Long size) {
    this.size = size;
  }

  public FileModel checksums(List<DRSChecksum> checksums) {
    this.checksums = checksums;
    return this;
  }

  public FileModel addChecksumsItem(DRSChecksum checksumsItem) {
    if (this.checksums == null) {
      this.checksums = new ArrayList<>();
    }
    this.checksums.add(checksumsItem);
    return this;
  }

   /**
   * Always present for files - checksums; always includes crc32c. May include md5. Present for directories in snapshots - see DRS spec for algorithm for combining checksums of underlying directory contents. 
   * @return checksums
  **/
  @ApiModelProperty(value = "Always present for files - checksums; always includes crc32c. May include md5. Present for directories in snapshots - see DRS spec for algorithm for combining checksums of underlying directory contents. ")
  public List<DRSChecksum> getChecksums() {
    return checksums;
  }

  public void setChecksums(List<DRSChecksum> checksums) {
    this.checksums = checksums;
  }

  public FileModel created(String created) {
    this.created = created;
    return this;
  }

   /**
   * timestamp of object creation in RFC3339
   * @return created
  **/
  @ApiModelProperty(value = "timestamp of object creation in RFC3339")
  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public FileModel description(String description) {
    this.description = description;
    return this;
  }

   /**
   * Human readable description of the file
   * @return description
  **/
  @ApiModelProperty(value = "Human readable description of the file")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public FileModel fileType(FileModelType fileType) {
    this.fileType = fileType;
    return this;
  }

   /**
   * Get fileType
   * @return fileType
  **/
  @ApiModelProperty(value = "")
  public FileModelType getFileType() {
    return fileType;
  }

  public void setFileType(FileModelType fileType) {
    this.fileType = fileType;
  }

  public FileModel fileDetail(FileDetailModel fileDetail) {
    this.fileDetail = fileDetail;
    return this;
  }

   /**
   * Get fileDetail
   * @return fileDetail
  **/
  @ApiModelProperty(value = "")
  public FileDetailModel getFileDetail() {
    return fileDetail;
  }

  public void setFileDetail(FileDetailModel fileDetail) {
    this.fileDetail = fileDetail;
  }

  public FileModel directoryDetail(DirectoryDetailModel directoryDetail) {
    this.directoryDetail = directoryDetail;
    return this;
  }

   /**
   * Get directoryDetail
   * @return directoryDetail
  **/
  @ApiModelProperty(value = "")
  public DirectoryDetailModel getDirectoryDetail() {
    return directoryDetail;
  }

  public void setDirectoryDetail(DirectoryDetailModel directoryDetail) {
    this.directoryDetail = directoryDetail;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileModel fileModel = (FileModel) o;
    return Objects.equals(this.fileId, fileModel.fileId) &&
        Objects.equals(this.collectionId, fileModel.collectionId) &&
        Objects.equals(this.path, fileModel.path) &&
        Objects.equals(this.size, fileModel.size) &&
        Objects.equals(this.checksums, fileModel.checksums) &&
        Objects.equals(this.created, fileModel.created) &&
        Objects.equals(this.description, fileModel.description) &&
        Objects.equals(this.fileType, fileModel.fileType) &&
        Objects.equals(this.fileDetail, fileModel.fileDetail) &&
        Objects.equals(this.directoryDetail, fileModel.directoryDetail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileId, collectionId, path, size, checksums, created, description, fileType, fileDetail, directoryDetail);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FileModel {\n");
    
    sb.append("    fileId: ").append(toIndentedString(fileId)).append("\n");
    sb.append("    collectionId: ").append(toIndentedString(collectionId)).append("\n");
    sb.append("    path: ").append(toIndentedString(path)).append("\n");
    sb.append("    size: ").append(toIndentedString(size)).append("\n");
    sb.append("    checksums: ").append(toIndentedString(checksums)).append("\n");
    sb.append("    created: ").append(toIndentedString(created)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    fileType: ").append(toIndentedString(fileType)).append("\n");
    sb.append("    fileDetail: ").append(toIndentedString(fileDetail)).append("\n");
    sb.append("    directoryDetail: ").append(toIndentedString(directoryDetail)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

