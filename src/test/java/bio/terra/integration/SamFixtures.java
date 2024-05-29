package bio.terra.integration;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.auth.AuthService;
import bio.terra.common.configuration.TestConfiguration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.AdminApi;
import org.broadinstitute.dsde.workbench.client.sam.api.GroupApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
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
    headers.setAccept(List.of(MediaType.APPLICATION_JSON));

    restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
  }

  public void deleteServiceAccountFromTerra(TestConfiguration.User user, String serviceAccount) {
    logger.info("Deleting user {} from Sam {}", serviceAccount, samConfig.basePath());
    try {
      // Get the user ID to delete
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      AdminApi samAdminApi = new AdminApi(getApiClient(accessToken));
      UserStatus userStatus = samAdminApi.adminGetUserByEmail(serviceAccount);

      logger.info(
          "Found user {} with id {}",
          userStatus.getUserInfo().getUserEmail(),
          userStatus.getUserInfo().getUserSubjectId());

      // Delete the user
      String userDeletionUrl =
          "%s/api/admin/v1/user/%s"
              .formatted(samConfig.basePath(), userStatus.getUserInfo().getUserSubjectId());
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

  public void deleteSnapshotAccessRequest(
      TestConfiguration.User user, UUID snapshotAccessRequestId) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      ResourcesApi samResourcesApi = new ResourcesApi(getApiClient(accessToken));
      samResourcesApi.deleteResourceV2(
          "snapshot-builder-request", snapshotAccessRequestId.toString());
      logger.info("Deleted snapshot access request {}", snapshotAccessRequestId);
    } catch (ApiException e) {
      throw new RuntimeException("Error deleting snapshot access request: %s", e);
    }
  }

  public void addGroup(TestConfiguration.User user, String groupName) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      GroupApi samGroupApi = new GroupApi(getApiClient(accessToken));
      // what is supposed to be in the body?
      samGroupApi.postGroup(groupName, new Object());
      logger.info("Created Sam Group {}", groupName);
    } catch (ApiException e) {
      throw new RuntimeException("Error creating Sam Group: %s", e);
    }
  }

  public void addUserToGroup(TestConfiguration.User user, String groupName, String policyName) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      GroupApi samGroupApi = new GroupApi(getApiClient(accessToken));
      samGroupApi.addEmailToGroup(groupName, policyName, user.getEmail(), new Object());
      logger.info(
          "Added User {} to Sam Group {} with policy name {} ",
          user.getEmail(),
          groupName,
          policyName);
    } catch (ApiException e) {
      throw new RuntimeException("Error adding user to Sam Group: %s", e);
    }
  }

  public List<String> getDataAccessControlsForResource(
      TestConfiguration.User user, String resourceType, String resourceId) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      ResourcesApi samResourcesApi = new ResourcesApi(getApiClient(accessToken));
      return samResourcesApi.getAuthDomainV2(resourceType, resourceId);
    } catch (ApiException e) {
      throw new RuntimeException("Error retrieving Data Access Controls: %s", e);
    }
  }

  private String getAccessToken(HttpHeaders authedHeader) {
    return Optional.ofNullable(
            Optional.ofNullable(authedHeader.get(HttpHeaders.AUTHORIZATION))
                .orElse(List.of())
                .iterator()
                .next())
        .map(h -> h.replaceAll("Bearer ", ""))
        .orElseThrow(() -> new IllegalArgumentException("No auth header present"));
  }

  private ApiClient getApiClient(String accessToken) {
    ApiClient apiClient = new ApiClient();
    apiClient.setAccessToken(accessToken);
    apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam
    return apiClient.setBasePath(samConfig.basePath());
  }

  private HttpHeaders getHeaders(TestConfiguration.User user) {
    HttpHeaders copy = new HttpHeaders(headers);
    copy.setBearerAuth(authService.getAuthToken(user.getEmail()));
    copy.set("From", user.getEmail());
    return copy;
  }
}
