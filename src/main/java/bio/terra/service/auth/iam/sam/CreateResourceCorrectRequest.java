package bio.terra.service.auth.iam.sam;

import com.google.gson.annotations.SerializedName;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;

// This is a work around for https://broadworkbench.atlassian.net/browse/AP-149
// This is basically a copy of CreateResourceRequest except the policies variable is the Map
// that the server expects
public class CreateResourceCorrectRequest {

  public static final String SERIALIZED_NAME_RESOURCE_ID = "resourceId";

  @SerializedName(SERIALIZED_NAME_RESOURCE_ID)
  private String resourceId;

  public static final String SERIALIZED_NAME_POLICIES = "policies";

  @SerializedName(SERIALIZED_NAME_POLICIES)
  private Map<String, AccessPolicyMembership> policies = new HashMap<>();

  public static final String SERIALIZED_NAME_AUTH_DOMAIN = "authDomain";

  @SerializedName(SERIALIZED_NAME_AUTH_DOMAIN)
  private List<String> authDomain = new ArrayList<String>();

  public CreateResourceCorrectRequest resourceId(String resourceId) {
    this.resourceId = resourceId;
    return this;
  }

  /**
   * id of the resource to create
   *
   * @return resourceId
   */
  @ApiModelProperty(required = true, value = "id of the resource to create")
  public String getResourceId() {
    return resourceId;
  }

  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  public CreateResourceCorrectRequest policies(Map<String, AccessPolicyMembership> policies) {
    this.policies = policies;
    return this;
  }

  public CreateResourceCorrectRequest addPoliciesItem(
      String policyName, AccessPolicyMembership policiesItem) {
    this.policies.put(policyName, policiesItem);
    return this;
  }

  /**
   * map of initial policies to create
   *
   * @return policies
   */
  @ApiModelProperty(required = true, value = "map of initial policies to create")
  public Map<String, AccessPolicyMembership> getPolicies() {
    return policies;
  }

  public void setPolicies(Map<String, AccessPolicyMembership> policies) {
    this.policies = policies;
  }

  public CreateResourceCorrectRequest authDomain(List<String> authDomain) {
    this.authDomain = authDomain;
    return this;
  }

  public CreateResourceCorrectRequest addAuthDomainItem(String authDomainItem) {
    if (this.authDomain == null) {
      this.authDomain = new ArrayList<String>();
    }
    this.authDomain.add(authDomainItem);
    return this;
  }

  /**
   * Get authDomain
   *
   * @return authDomain
   */
  @ApiModelProperty(value = "")
  public List<String> getAuthDomain() {
    return authDomain;
  }

  public void setAuthDomain(List<String> authDomain) {
    this.authDomain = authDomain;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateResourceCorrectRequest createResourceRequest = (CreateResourceCorrectRequest) o;
    return Objects.equals(this.resourceId, createResourceRequest.resourceId)
        && Objects.equals(this.policies, createResourceRequest.policies)
        && Objects.equals(this.authDomain, createResourceRequest.authDomain);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceId, policies, authDomain);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateResourceCorrectRequest {\n");
    sb.append("    resourceId: ").append(toIndentedString(resourceId)).append("\n");
    sb.append("    policies: ").append(toIndentedString(policies)).append("\n");
    sb.append("    authDomain: ").append(toIndentedString(authDomain)).append("\n");
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
