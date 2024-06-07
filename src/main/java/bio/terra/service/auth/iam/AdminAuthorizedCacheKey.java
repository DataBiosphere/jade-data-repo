package bio.terra.service.auth.iam;

import bio.terra.common.iam.AuthenticatedUserRequest;

public record AdminAuthorizedCacheKey(
    AuthenticatedUserRequest userReq, IamResourceType iamResourceType, IamAction action) {}
