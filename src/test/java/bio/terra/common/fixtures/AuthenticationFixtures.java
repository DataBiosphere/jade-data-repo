package bio.terra.common.fixtures;

import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.UUID;

public final class AuthenticationFixtures {

  /**
   * @return an AuthenticatedUserRequest for tests with a randomly generated token
   */
  public static AuthenticatedUserRequest randomUserRequest() {
    return AuthenticatedUserRequest.builder()
        .setSubjectId("subjectid")
        .setEmail("email")
        .setToken(UUID.randomUUID().toString())
        .build();
  }
}
