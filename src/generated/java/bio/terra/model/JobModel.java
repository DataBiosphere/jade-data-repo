package bio.terra.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.validation.annotation.Validated;
import javax.validation.Valid;
import javax.validation.constraints.*;

/**
 * Status of job 
 */
@ApiModel(description = "Status of job ")
@Validated
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2019-02-07T10:34:40.027-05:00")

public class JobModel   {
  @JsonProperty("id")
  private String id = null;

  @JsonProperty("description")
  private String description = null;

  /**
   * Status of job
   */
  public enum StatusEnum {
    RUNNING("running"),
    
    SUCCEEDED("succeeded"),
    
    FAILED("failed");

    private String value;

    StatusEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static StatusEnum fromValue(String text) {
      for (StatusEnum b : StatusEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("status")
  private StatusEnum status = null;

  @JsonProperty("submitted")
  private String submitted = null;

  @JsonProperty("completed")
  private String completed = null;

  public JobModel id(String id) {
    this.id = id;
    return this;
  }

  /**
   * Get id
   * @return id
  **/
  @ApiModelProperty(required = true, value = "")
  @NotNull


  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public JobModel description(String description) {
    this.description = description;
    return this;
  }

  /**
   * Description of the job's flight
   * @return description
  **/
  @ApiModelProperty(value = "Description of the job's flight")


  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public JobModel status(StatusEnum status) {
    this.status = status;
    return this;
  }

  /**
   * Status of job
   * @return status
  **/
  @ApiModelProperty(required = true, value = "Status of job")
  @NotNull


  public StatusEnum getStatus() {
    return status;
  }

  public void setStatus(StatusEnum status) {
    this.status = status;
  }

  public JobModel submitted(String submitted) {
    this.submitted = submitted;
    return this;
  }

  /**
   * Timestamp when the flight was created
   * @return submitted
  **/
  @ApiModelProperty(required = true, value = "Timestamp when the flight was created")
  @NotNull


  public String getSubmitted() {
    return submitted;
  }

  public void setSubmitted(String submitted) {
    this.submitted = submitted;
  }

  public JobModel completed(String completed) {
    this.completed = completed;
    return this;
  }

  /**
   * Timestamp when the flight was completed; not present if not complete
   * @return completed
  **/
  @ApiModelProperty(value = "Timestamp when the flight was completed; not present if not complete")


  public String getCompleted() {
    return completed;
  }

  public void setCompleted(String completed) {
    this.completed = completed;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobModel jobModel = (JobModel) o;
    return Objects.equals(this.id, jobModel.id) &&
        Objects.equals(this.description, jobModel.description) &&
        Objects.equals(this.status, jobModel.status) &&
        Objects.equals(this.submitted, jobModel.submitted) &&
        Objects.equals(this.completed, jobModel.completed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, description, status, submitted, completed);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class JobModel {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    description: ").append(toIndentedString(description)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    submitted: ").append(toIndentedString(submitted)).append("\n");
    sb.append("    completed: ").append(toIndentedString(completed)).append("\n");
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

