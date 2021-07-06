package bio.terra.service.iam;

import bio.terra.app.configuration.ApplicationConfiguration;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
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

    Optional<String> token =
        Optional.ofNullable(req.getHeader("Authorization"))
            .map(header -> header.substring("Bearer ".length()));

    String email =
        Optional.ofNullable(req.getHeader("From")).orElse(applicationConfiguration.getUserEmail());

    String userId = applicationConfiguration.getUserId();

    return new AuthenticatedUserRequest().email(email).subjectId(userId).token(token);
  }
}
