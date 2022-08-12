package bio.terra.service.auth.iam;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.auth.oauth2.GoogleOauthUtils;
import bio.terra.service.auth.oauth2.exceptions.OauthTokeninfoRetrieveException;
import com.google.api.services.oauth2.model.Tokeninfo;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Primary
@Component
@Profile({"local", "connectedtest", "unittest"})
public class LocalAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {
  private Logger logger = LoggerFactory.getLogger(LocalAuthenticatedUserRequestFactory.class);

  // in testing scenarios and when running the server without the proxy not all the
  // header information will be available. default values will be used in these cases.

  private final ApplicationConfiguration applicationConfiguration;

  @Autowired
  public LocalAuthenticatedUserRequestFactory(ApplicationConfiguration applicationConfiguration) {
    this.applicationConfiguration = applicationConfiguration;
  }

  // Static method to build an AuthenticatedUserRequest from data available to the controller
  public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
    HttpServletRequest req = servletRequest;

    String token =
        Optional.ofNullable(req.getHeader("Authorization"))
            .map(header -> header.substring("Bearer ".length()))
            .orElse("");
    String email =
        Optional.ofNullable(req.getHeader("From")).orElse(applicationConfiguration.getUserEmail());
    String userId = applicationConfiguration.getUserId();

    if (StringUtils.isNotEmpty(token)) {
      try {
        Tokeninfo tokenInfo = GoogleOauthUtils.getOauth2TokenInfo(token);
        email = tokenInfo.getEmail();
        userId = tokenInfo.getUserId();
      } catch (OauthTokeninfoRetrieveException ex) {
        logger.info("Error parsing auth token");
      }
    }

    return AuthenticatedUserRequest.builder()
        .setEmail(email)
        .setSubjectId(userId)
        .setToken(token)
        .build();
  }
}
