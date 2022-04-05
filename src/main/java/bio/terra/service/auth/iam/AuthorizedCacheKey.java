package bio.terra.service.auth.iam;

import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.Objects;

public class AuthorizedCacheKey {
  // purpose of this object is to contain 4 objects
  private AuthenticatedUserRequest userReq;
  private IamResourceType iamResourceType;
  private String resourceId;
  private IamAction action;

  public AuthorizedCacheKey(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action) {
    this.userReq = userReq;
    this.iamResourceType = iamResourceType;
    this.resourceId = resourceId;
    this.action = action;
  }

  public AuthenticatedUserRequest getUserReq() {
    return userReq;
  }

  public IamResourceType getIamResourceType() {
    return iamResourceType;
  }

  public String getResourceId() {
    return resourceId;
  }

  public IamAction getAction() {
    return action;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthorizedCacheKey)) {
      return false;
    }
    AuthorizedCacheKey that = (AuthorizedCacheKey) o;
    return Objects.equals(getUserReq(), that.getUserReq())
        && getIamResourceType() == that.getIamResourceType()
        && Objects.equals(getResourceId(), that.getResourceId())
        && getAction() == that.getAction();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getUserReq(), getIamResourceType(), getResourceId(), getAction());
  }
}
