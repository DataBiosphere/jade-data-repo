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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Representation of an error that occurred while processing a request. Some errors provide error details. For example, validation of input may report several problems. Each of those are reported in a separate error detail. 
 */
@ApiModel(description = "Representation of an error that occurred while processing a request. Some errors provide error details. For example, validation of input may report several problems. Each of those are reported in a separate error detail. ")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class ErrorModel {
  @JsonProperty("message")
  private String message = null;

  @JsonProperty("errorDetail")
  private List<String> errorDetail = null;

  public ErrorModel message(String message) {
    this.message = message;
    return this;
  }

   /**
   * Get message
   * @return message
  **/
  @ApiModelProperty(required = true, value = "")
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ErrorModel errorDetail(List<String> errorDetail) {
    this.errorDetail = errorDetail;
    return this;
  }

  public ErrorModel addErrorDetailItem(String errorDetailItem) {
    if (this.errorDetail == null) {
      this.errorDetail = new ArrayList<>();
    }
    this.errorDetail.add(errorDetailItem);
    return this;
  }

   /**
   * Get errorDetail
   * @return errorDetail
  **/
  @ApiModelProperty(value = "")
  public List<String> getErrorDetail() {
    return errorDetail;
  }

  public void setErrorDetail(List<String> errorDetail) {
    this.errorDetail = errorDetail;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ErrorModel errorModel = (ErrorModel) o;
    return Objects.equals(this.message, errorModel.message) &&
        Objects.equals(this.errorDetail, errorModel.errorDetail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(message, errorDetail);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ErrorModel {\n");
    
    sb.append("    message: ").append(toIndentedString(message)).append("\n");
    sb.append("    errorDetail: ").append(toIndentedString(errorDetail)).append("\n");
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

