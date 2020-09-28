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
import bio.terra.datarepo.model.BulkLoadFileState;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Describes the status result of one file within a bulk file load
 */
@ApiModel(description = "Describes the status result of one file within a bulk file load")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class BulkLoadFileResultModel {
  @JsonProperty("sourcePath")
  private String sourcePath = null;

  @JsonProperty("targetPath")
  private String targetPath = null;

  @JsonProperty("state")
  private BulkLoadFileState state = null;

  @JsonProperty("fileId")
  private String fileId = null;

  @JsonProperty("error")
  private String error = null;

  public BulkLoadFileResultModel sourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
    return this;
  }

   /**
   * gs URL of the source file to load
   * @return sourcePath
  **/
  @ApiModelProperty(required = true, value = "gs URL of the source file to load")
  public String getSourcePath() {
    return sourcePath;
  }

  public void setSourcePath(String sourcePath) {
    this.sourcePath = sourcePath;
  }

  public BulkLoadFileResultModel targetPath(String targetPath) {
    this.targetPath = targetPath;
    return this;
  }

   /**
   * Full path within the dataset where the file should be placed. The path must start with /. 
   * @return targetPath
  **/
  @ApiModelProperty(required = true, value = "Full path within the dataset where the file should be placed. The path must start with /. ")
  public String getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(String targetPath) {
    this.targetPath = targetPath;
  }

  public BulkLoadFileResultModel state(BulkLoadFileState state) {
    this.state = state;
    return this;
  }

   /**
   * Get state
   * @return state
  **/
  @ApiModelProperty(value = "")
  public BulkLoadFileState getState() {
    return state;
  }

  public void setState(BulkLoadFileState state) {
    this.state = state;
  }

  public BulkLoadFileResultModel fileId(String fileId) {
    this.fileId = fileId;
    return this;
  }

   /**
   * The fileId of the loaded file; non-null if state is SUCCEEDED
   * @return fileId
  **/
  @ApiModelProperty(value = "The fileId of the loaded file; non-null if state is SUCCEEDED")
  public String getFileId() {
    return fileId;
  }

  public void setFileId(String fileId) {
    this.fileId = fileId;
  }

  public BulkLoadFileResultModel error(String error) {
    this.error = error;
    return this;
  }

   /**
   * The error message if state is FAILED
   * @return error
  **/
  @ApiModelProperty(value = "The error message if state is FAILED")
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BulkLoadFileResultModel bulkLoadFileResultModel = (BulkLoadFileResultModel) o;
    return Objects.equals(this.sourcePath, bulkLoadFileResultModel.sourcePath) &&
        Objects.equals(this.targetPath, bulkLoadFileResultModel.targetPath) &&
        Objects.equals(this.state, bulkLoadFileResultModel.state) &&
        Objects.equals(this.fileId, bulkLoadFileResultModel.fileId) &&
        Objects.equals(this.error, bulkLoadFileResultModel.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourcePath, targetPath, state, fileId, error);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class BulkLoadFileResultModel {\n");
    
    sb.append("    sourcePath: ").append(toIndentedString(sourcePath)).append("\n");
    sb.append("    targetPath: ").append(toIndentedString(targetPath)).append("\n");
    sb.append("    state: ").append(toIndentedString(state)).append("\n");
    sb.append("    fileId: ").append(toIndentedString(fileId)).append("\n");
    sb.append("    error: ").append(toIndentedString(error)).append("\n");
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

