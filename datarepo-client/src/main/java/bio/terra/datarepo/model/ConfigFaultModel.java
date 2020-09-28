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
import bio.terra.datarepo.model.ConfigFaultCountedModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * Fault control parameters
 */
@ApiModel(description = "Fault control parameters")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaClientCodegen", date = "2020-09-25T14:51:02.312-04:00")
public class ConfigFaultModel {
  @JsonProperty("enabled")
  private Boolean enabled = null;

  /**
   * A simple fault has no parameters. It is just enabled or disabled. This type of fault is typically used when the desired behavior of the fault is too complex for expression in the fault types and custom code is needed to get the right failure behavior.  A counted fault is used to insert some number of faults in a pattern. See the ConfigFaultCountedModel for details. 
   */
  public enum FaultTypeEnum {
    SIMPLE("simple"),
    
    COUNTED("counted");

    private String value;

    FaultTypeEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static FaultTypeEnum fromValue(String text) {
      for (FaultTypeEnum b : FaultTypeEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("faultType")
  private FaultTypeEnum faultType = null;

  @JsonProperty("counted")
  private ConfigFaultCountedModel counted = null;

  public ConfigFaultModel enabled(Boolean enabled) {
    this.enabled = enabled;
    return this;
  }

   /**
   * If the fault is enabled, then is in effect. Fault points cause insert faults. Typical usage is that faults are disabled on system start and explicitly enabled by test code or via the setFault endpoint. 
   * @return enabled
  **/
  @ApiModelProperty(value = "If the fault is enabled, then is in effect. Fault points cause insert faults. Typical usage is that faults are disabled on system start and explicitly enabled by test code or via the setFault endpoint. ")
  public Boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public ConfigFaultModel faultType(FaultTypeEnum faultType) {
    this.faultType = faultType;
    return this;
  }

   /**
   * A simple fault has no parameters. It is just enabled or disabled. This type of fault is typically used when the desired behavior of the fault is too complex for expression in the fault types and custom code is needed to get the right failure behavior.  A counted fault is used to insert some number of faults in a pattern. See the ConfigFaultCountedModel for details. 
   * @return faultType
  **/
  @ApiModelProperty(value = "A simple fault has no parameters. It is just enabled or disabled. This type of fault is typically used when the desired behavior of the fault is too complex for expression in the fault types and custom code is needed to get the right failure behavior.  A counted fault is used to insert some number of faults in a pattern. See the ConfigFaultCountedModel for details. ")
  public FaultTypeEnum getFaultType() {
    return faultType;
  }

  public void setFaultType(FaultTypeEnum faultType) {
    this.faultType = faultType;
  }

  public ConfigFaultModel counted(ConfigFaultCountedModel counted) {
    this.counted = counted;
    return this;
  }

   /**
   * Get counted
   * @return counted
  **/
  @ApiModelProperty(value = "")
  public ConfigFaultCountedModel getCounted() {
    return counted;
  }

  public void setCounted(ConfigFaultCountedModel counted) {
    this.counted = counted;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigFaultModel configFaultModel = (ConfigFaultModel) o;
    return Objects.equals(this.enabled, configFaultModel.enabled) &&
        Objects.equals(this.faultType, configFaultModel.faultType) &&
        Objects.equals(this.counted, configFaultModel.counted);
  }

  @Override
  public int hashCode() {
    return Objects.hash(enabled, faultType, counted);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigFaultModel {\n");
    
    sb.append("    enabled: ").append(toIndentedString(enabled)).append("\n");
    sb.append("    faultType: ").append(toIndentedString(faultType)).append("\n");
    sb.append("    counted: ").append(toIndentedString(counted)).append("\n");
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

