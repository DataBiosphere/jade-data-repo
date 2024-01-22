package bio.terra.service.resourcemanagement;

import bio.terra.app.configuration.ResourceBufferServiceConfiguration;
import bio.terra.buffer.api.BufferApi;
import bio.terra.buffer.api.UnauthenticatedApi;
import bio.terra.buffer.client.ApiClient;
import bio.terra.buffer.client.ApiException;
import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.buffer.model.SystemStatus;
import bio.terra.buffer.model.SystemStatusSystems;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
import bio.terra.service.resourcemanagement.exception.BufferServiceAuthorizationException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/** A service for integrating with the Resource Buffer Service. */
@Component
public class BufferService {
  private static final Logger logger = LoggerFactory.getLogger(BufferService.class);

  private static final String GCS_FOLDER_TYPE = "folder";

  private final ResourceBufferServiceConfiguration bufferServiceConfiguration;
  private final GoogleResourceConfiguration googleConfig;
  private final GoogleResourceManagerService googleResourceManagerService;
  /** Clients should be shared among requests to reduce latency and save memory * */
  private final Client sharedHttpClient;

  @Autowired
  public BufferService(
      ResourceBufferServiceConfiguration bufferServiceConfiguration,
      GoogleResourceConfiguration googleConfig,
      GoogleResourceManagerService googleResourceManagerService) {
    this.bufferServiceConfiguration = bufferServiceConfiguration;
    this.googleConfig = googleConfig;
    this.googleResourceManagerService = googleResourceManagerService;
    this.sharedHttpClient = new ApiClient().getHttpClient();
  }

  private ApiClient createUnauthApiClient() {
    return new ApiClient()
        .setHttpClient(sharedHttpClient)
        .setBasePath(bufferServiceConfiguration.instanceUrl());
  }

  private ApiClient createApiClient(String accessToken) {
    ApiClient client = createUnauthApiClient();
    client.setAccessToken(accessToken);
    return client;
  }

  private BufferApi bufferApi() throws IOException {
    return new BufferApi(createApiClient(bufferServiceConfiguration.getAccessToken()));
  }

  /**
   * Retrieve a single resource from the Buffer Service. The instance and pool are already
   * configured.
   *
   * @return ResourceInfo
   */
  public ResourceInfo handoutResource(boolean enableSecureMonitoring) {
    String handoutRequestId = UUID.randomUUID().toString();
    logger.info("Using request ID: {} to get project from RBS", handoutRequestId);
    HandoutRequestBody requestBody = new HandoutRequestBody().handoutRequestId(handoutRequestId);
    try {
      BufferApi bufferApi = bufferApi();
      ResourceInfo info =
          bufferApi.handoutResource(requestBody, bufferServiceConfiguration.poolId());
      logger.info(
          "Retrieved resource from pool {} on Buffer Service instance {}",
          bufferServiceConfiguration.poolId(),
          bufferServiceConfiguration.instanceUrl());

      if (enableSecureMonitoring) {
        var projectId = info.getCloudResourceUid().getGoogleProjectUid().getProjectId();
        try {
          refolderProjectToSecureFolder(projectId);
        } catch (IOException | GeneralSecurityException e) {
          deleteProject(projectId);
          throw new GoogleResourceException("Could not re-folder new project", e);
        }
      }

      return info;
    } catch (IOException e) {
      throw new BufferServiceAuthorizationException("Error reading or parsing credentials file", e);
    } catch (ApiException e) {
      if (e.getCode() == HttpStatus.UNAUTHORIZED.value()) {
        throw new BufferServiceAuthorizationException("Not authorized to access Buffer Service", e);
      } else {
        // TODO - remove this log
        // Added to debug issue we're seeing 'Failed to construct exception' error on
        // BufferServiceAPIException
        logger.error(
            "RBS ApiException on handoutResource; Response Body: "
                + e.getResponseBody()
                + "; Code: "
                + e.getCode());
        throw new BufferServiceAPIException(e);
      }
    } catch (GeneralSecurityException e) {
      throw new GoogleResourceException("Failed to cleanup after failing to re-folder project", e);
    }
  }

  public void refolderProjectToSecureFolder(String projectId)
      throws IOException, GeneralSecurityException {
    refolderProject(projectId, googleConfig.secureFolderResourceId(), "secure");
  }

  public void refolderProjectToDefaultFolder(String projectId)
      throws IOException, GeneralSecurityException {
    refolderProject(projectId, googleConfig.defaultFolderResourceId(), "default");
  }

  public void refolderProject(String projectId, String targetFolderId, String folderType)
      throws IOException, GeneralSecurityException {
    CloudResourceManager cloudResourceManager = googleResourceManagerService.cloudResourceManager();
    var project = cloudResourceManager.projects().get(projectId).execute();

    if (project.getParent().getId().equals(targetFolderId)) {
      logger.info("Project {} is already in {} folder", projectId, folderType);
      return;
    }
    ResourceId resourceId = new ResourceId().setType(GCS_FOLDER_TYPE).setId(targetFolderId);

    project.setParent(resourceId);

    cloudResourceManager.projects().update(projectId, project).execute();
    logger.info("Project {} moved to {} folder", projectId, folderType);
  }

  private void deleteProject(String projectId) throws IOException, GeneralSecurityException {
    CloudResourceManager cloudResourceManager = googleResourceManagerService.cloudResourceManager();
    cloudResourceManager.projects().delete(projectId).execute();
  }

  public RepositoryStatusModelSystems status() {
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(createUnauthApiClient());
    try {
      SystemStatus status = unauthenticatedApi.serviceStatus();
      Map<String, SystemStatusSystems> subsystemStatusMap = status.getSystems();
      return new RepositoryStatusModelSystems()
          .ok(status.isOk())
          .critical(bufferServiceConfiguration.enabled())
          .message(subsystemStatusMap.toString());
    } catch (ApiException e) {
      return new RepositoryStatusModelSystems()
          .ok(false)
          .critical(bufferServiceConfiguration.enabled())
          .message(e.getResponseBody());
    }
  }
}
