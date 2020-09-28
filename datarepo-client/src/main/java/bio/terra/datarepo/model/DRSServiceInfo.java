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

/**
 * Useful information about the running service.
 */
@ApiModel(description = "Useful information about the running service.")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class DRSServiceInfo {
  @JsonProperty("version")
  private String version = null;

  @JsonProperty("title")
  private String title = null;

  @JsonProperty("description")
  private String description = null;

  @JsonProperty("contact")
  private Object contact = null;

  @JsonProperty("license")
  private Object license = null;

  public DRSServiceInfo version(String version) {
    this.version = version;
    return this;
  }

   /**
   * Service version
   * @return version
  **/
  @ApiModelProperty(required = true, value = "Service version")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public DRSServiceInfo title(String title) {
    this.title = title;
    return this;
  }

   /**
   * Service name
   * @return title
  **/
  @ApiModelProperty(value = "Service name")
  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public DRSServiceInfo description(String description) {
    this.description = description;
    return this;
  }

   /**
   * Service description
   * @return description
  **/
  @ApiModelProperty(value = "Service description")
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public DRSServiceInfo contact(Object contact) {
    this.contact = contact;
    return this;
  }

   /**
   * Maintainer contact info
   * @return contact
  **/
  @ApiModelProperty(value = "Maintainer contact info")
  public Object getContact() {
    return contact;
  }

  public void setContact(Object contact) {
    this.contact = contact;
  }

  public DRSServiceInfo license(Object license) {
    this.license = license;
    return this;
  }

   /**
   * License information for the exposed API
   * @return license
  **/
  @ApiModelProperty(value = "License information for the exposed API")
  public Object getLicense() {
    return license;
  }

  public void setLicense(Object license) {
    this.license = license;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DRSServiceInfo drSServiceInfo = (DRSServiceInfo) o;
    return Objects.equals(this.version, drSServiceInfo.version) &&
        Objects.equals(this.title, drSServiceInfo.title) &&
        Objects.equals(this.description, drSServiceInfo.description) &&
        Objects.equals(this.contact, drSServiceInfo.contact) &&
        Objects.equals(this.license, drSServiceInfo.license);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, title, description, contact, license);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DRSServiceInfo {\n");
    
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    title: ").append(toIndentedString(title)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    contact: ").append(toIndentedString(contact)).append("\n");
    sb.append("    license: ").append(toIndentedString(license)).append("\n");
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

