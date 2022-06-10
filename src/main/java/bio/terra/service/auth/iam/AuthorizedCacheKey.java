package bio.terra.service.auth.iam;

import bio.terra.common.iam.AuthenticatedUserRequest;

public record AuthorizedCacheKey(
    AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId) {}
