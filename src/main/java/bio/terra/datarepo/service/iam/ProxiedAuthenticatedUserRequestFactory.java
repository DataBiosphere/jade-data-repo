package bio.terra.datarepo.service.iam;

import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Primary
@Profile({"terra", "dev", "integration"})
@Component
public class ProxiedAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {

  // Method to build an AuthenticatedUserRequest from data available to the controller
  public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
    String token =
        Optional.ofNullable(servletRequest.getHeader("oidc_access_token"))
            .orElseGet(
                () -> {
                  String authHeader = servletRequest.getHeader("Authorization");
                  return StringUtils.substring(authHeader, "Bearer:".length());
                });
    return new AuthenticatedUserRequest()
        .email(servletRequest.getHeader("oidc_claim_email"))
        .subjectId(servletRequest.getHeader("oidc_claim_user_id"))
        .token(Optional.ofNullable(token));
  }
}
