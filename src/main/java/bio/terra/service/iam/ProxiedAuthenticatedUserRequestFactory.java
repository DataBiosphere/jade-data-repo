package bio.terra.service.iam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Primary
@Profile({"terra", "dev", "integration"})
@Component
public class ProxiedAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {
  private Logger logger = LoggerFactory.getLogger(ProxiedAuthenticatedUserRequestFactory.class);

  @Autowired private ObjectMapper objectMapper;

  // Method to build an AuthenticatedUserRequest from data available to the controller
  public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
    String token =
        Optional.ofNullable(servletRequest.getHeader("oath2_claim_access_token"))
            .orElseGet(
                () -> {
                  String authHeader = servletRequest.getHeader("Authorization");
                  return StringUtils.substring(authHeader, "Bearer:".length());
                });
    try {
      // Debug: enable seeing what headers are forwarded from the proxy
      logger.info(
          "headers: {}",
          objectMapper
              .writerWithDefaultPrettyPrinter()
              .writeValueAsString(
                  Collections.list(servletRequest.getHeaderNames()).stream()
                      .collect(Collectors.toMap(e -> e, servletRequest::getHeader))));
    } catch (JsonProcessingException e) {
      logger.error("Error parsing header", e);
    }

    return new AuthenticatedUserRequest()
        .email(servletRequest.getHeader("oauth2_claim_email"))
        .subjectId(servletRequest.getHeader("oauth2_claim_sub"))
        .token(Optional.ofNullable(token));
  }
}
