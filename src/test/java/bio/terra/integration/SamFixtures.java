package bio.terra.integration;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.auth.AuthService;
import bio.terra.common.configuration.TestConfiguration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.AdminApi;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
public class SamFixtures {

  private static final Logger logger = LoggerFactory.getLogger(SamFixtures.class);
  @Autowired private SamConfiguration samConfig;
  @Autowired private AuthService authService;
  private final HttpHeaders headers;
  private final RestTemplate restTemplate;

  public SamFixtures() {
    headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON));

    restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
  }

  public void deleteServiceAccountFromTerra(TestConfiguration.User user, String serviceAccount) {
    logger.info("Deleting user {}", serviceAccount);
    try {
      // Get the user ID to delete
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken =
          Optional.ofNullable(
                  Optional.ofNullable(authedHeader.get(HttpHeaders.AUTHORIZATION))
                      .orElse(List.of())
                      .iterator()
                      .next())
              .map(h -> h.replaceAll("Bearer ", ""))
              .orElseThrow(() -> new IllegalArgumentException("No auth header present"));
      AdminApi samAdminApi = new AdminApi(getApiClient(accessToken));
      UserStatus userStatus = samAdminApi.adminGetUserByEmail(serviceAccount);

      logger.info(
          "Found user {} with id {}",
          userStatus.getUserInfo().getUserEmail(),
          userStatus.getUserInfo().getUserSubjectId());

      // Delete the user
      String userDeletionUrl =
          "%s/api/admin/v1/user/%s"
              .formatted(samConfig.getBasePath(), userStatus.getUserInfo().getUserSubjectId());
      try {
        restTemplate.exchange(
            userDeletionUrl, HttpMethod.DELETE, new HttpEntity<>(null, authedHeader), Void.class);
      } catch (HttpClientErrorException e) {
        logger.error(
            "Error deleting user {} with id {} using user {} with url {}",
            userStatus.getUserInfo().getUserEmail(),
            userStatus.getUserInfo().getUserSubjectId(),
            user,
            userDeletionUrl);
        throw e;
      }
    } catch (ApiException e) {
      throw new RuntimeException(
          "Error deleting account %s from Terra".formatted(serviceAccount), e);
    }
  }

  private ApiClient getApiClient(String accessToken) {
    ApiClient apiClient = new ApiClient();
    apiClient.setAccessToken(accessToken);
    apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam
    return apiClient.setBasePath(samConfig.getBasePath());
  }

  private HttpHeaders getHeaders(TestConfiguration.User user) {
    HttpHeaders copy = new HttpHeaders(headers);
    copy.setBearerAuth(authService.getAuthToken(user.getEmail()));
    copy.set("From", user.getEmail());
    return copy;
  }
}
