package bio.terra.integration;

import static bio.terra.service.auth.iam.sam.SamIam.convertSamExToDataRepoEx;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.auth.AuthService;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.AdminApi;
import org.broadinstitute.dsde.workbench.client.sam.api.GroupApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipRequest;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
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
    deleteResource(
        user,
        new Resource(IamResourceType.SNAPSHOT_BUILDER_REQUEST, snapshotAccessRequestId.toString()));
  }

  public void addGroup(TestConfiguration.User user, String groupName) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      GroupApi samGroupApi = new GroupApi(getApiClient(accessToken));
      samGroupApi.postGroup(groupName, null);
      logger.info("Created Sam Group {}", groupName);
    } catch (ApiException e) {
      throw new RuntimeException("Error creating Sam Group: %s", e);
    }
  }

  public List<String> getAuthDomainForResource(
      TestConfiguration.User user, String resourceType, String resourceId) {
    try {
      ResourcesApi samResourcesApi = getResourcesApi(user);
      return samResourcesApi.getAuthDomainV2(resourceType, resourceId);
    } catch (ApiException e) {
      throw new RuntimeException("Error retrieving Data Access Controls: %s", e);
    }
  }

  public String getGroup(TestConfiguration.User user, String groupName) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      GroupApi samGroupApi = new GroupApi(getApiClient(accessToken));
      return samGroupApi.getGroup(groupName);
    } catch (ApiException e) {
      throw convertSamExToDataRepoEx(e);
    }
  }

  public void deleteGroup(TestConfiguration.User user, String groupName) {
    try {
      HttpHeaders authedHeader = getHeaders(user);
      String accessToken = getAccessToken(authedHeader);
      GroupApi samGroupApi = new GroupApi(getApiClient(accessToken));
      samGroupApi.deleteGroup(groupName);
      logger.info("Deleted Sam Group {}", groupName);
    } catch (ApiException e) {
      throw new RuntimeException("Error deleting Sam Group: %s", e);
    }
  }

  public record Resource(IamResourceType type, String id) {
    Resource(IamResourceType type) {
      this(type, UUID.randomUUID().toString());
    }

    FullyQualifiedResourceId toFQRI() {
      return new FullyQualifiedResourceId().resourceTypeName(type.toString()).resourceId(id);
    }
  }

  public void createResource(TestConfiguration.User user, Resource resource) {
    createResource(user, resource, null);
  }

  public void createResource(TestConfiguration.User user, Resource resource, Resource parent) {
    try {
      var request =
          new CreateResourceRequestV2()
              .resourceId(resource.id)
              .policies(
                  Map.of(
                      IamRole.STEWARD.toString(),
                      new AccessPolicyMembershipRequest()
                          .roles(List.of(IamRole.STEWARD.toString()))
                          .memberEmails(List.of(user.getEmail()))));
      if (parent != null) {
        request.setParent(parent.toFQRI());
      }
      ResourcesApi samResourcesApi = getResourcesApi(user);
      samResourcesApi.createResourceV2(resource.type.toString(), request);
      logger.info("Created {}", resource);
    } catch (ApiException e) {
      throw new RuntimeException("Error creating Sam resource: %s".formatted(resource), e);
    }
  }

  public void deleteResource(TestConfiguration.User user, Resource resource) {
    try {
      ResourcesApi samResourcesApi = getResourcesApi(user);
      samResourcesApi.deleteResourceV2(resource.type.toString(), resource.id);
      logger.info("Deleted {}", resource);
    } catch (ApiException e) {
      throw new RuntimeException("Error deleting Sam resource: %s".formatted(resource), e);
    }
  }

  public void addUserToResource(TestConfiguration.User user, Resource resource, IamRole role) {
    try {
      ResourcesApi samResourcesApi = getResourcesApi(user);
      samResourcesApi.addUserToPolicyV2(
          resource.type.toString(), resource.id, role.toString(), user.getEmail(), null);
      logger.info("Added user {} with role {} to {}", user, role, resource);
    } catch (ApiException e) {
      throw new RuntimeException("Error adding user to Sam resource: %s".formatted(resource), e);
    }
  }

  private ResourcesApi getResourcesApi(TestConfiguration.User user) {
    return new ResourcesApi(getApiClient(getAccessToken(getHeaders(user))));
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
