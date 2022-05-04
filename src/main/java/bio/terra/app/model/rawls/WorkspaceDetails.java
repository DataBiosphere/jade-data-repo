/*
 * Rawls
 * Rawls API
 *
 * OpenAPI spec version: 1.0.0
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */

package bio.terra.app.model.rawls;

import com.google.gson.annotations.SerializedName;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;

/** WorkspaceDetails */
@javax.annotation.Generated(
    value = "io.swagger.codegen.v3.generators.java.JavaClientCodegen",
    date = "2022-05-04T09:59:36.022590-04:00[America/New_York]")
public class WorkspaceDetails {

  @SerializedName("name")
  private String name = null;

  @SerializedName("namespace")
  private String namespace = null;

  @SerializedName("workspaceId")
  private String workspaceId = null;

  public WorkspaceDetails name(String name) {
    this.name = name;
    return this;
  }

  /**
   * The name of the workspace
   *
   * @return name
   */
  @Schema(required = true, description = "The name of the workspace")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public WorkspaceDetails namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * The namespace the workspace belongs to
   *
   * @return namespace
   */
  @Schema(required = true, description = "The namespace the workspace belongs to")
  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public WorkspaceDetails workspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
    return this;
  }

  /**
   * A UUID associated with the workspace
   *
   * @return workspaceId
   */
  @Schema(required = true, description = "A UUID associated with the workspace")
  public String getWorkspaceId() {
    return workspaceId;
  }

  public void setWorkspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkspaceDetails workspaceDetails = (WorkspaceDetails) o;
    return Objects.equals(this.name, workspaceDetails.name)
        && Objects.equals(this.namespace, workspaceDetails.namespace)
        && Objects.equals(this.workspaceId, workspaceDetails.workspaceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, namespace, workspaceId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkspaceDetails {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    namespace: ").append(toIndentedString(namespace)).append("\n");
    sb.append("    workspaceId: ").append(toIndentedString(workspaceId)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
