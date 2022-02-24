package bio.terra.service.profile.azure;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AzureAuthzService {
  private static final Logger logger = LoggerFactory.getLogger(AzureAuthzService.class);

  static final String AUTH_PARAM_KEY = "authorizedTDRUser";

  private final AzureResourceConfiguration resourceConfiguration;

  @Autowired
  public AzureAuthzService(AzureResourceConfiguration resourceConfiguration) {
    this.resourceConfiguration = resourceConfiguration;
  }

  public boolean canAccess(
      AuthenticatedUserRequest user,
      UUID subscriptionId,
      String resourceGroupName,
      String applicationDeploymentName,
      String apiVersion) {
    logger.info(
        "canAccess - subscriptionId: {}, resourceGroupName: {}, applicationDeploymentName: {}",
        subscriptionId,
        resourceGroupName,
        applicationDeploymentName);
    AzureResourceManager client = resourceConfiguration.getClient(subscriptionId);
    logger.info("Client: {}", client);
    logger.info("Client generic resources: {}", client.genericResources().list().stream().count());
    String applicationResourceId =
        MetadataDataAccessUtils.getApplicationDeploymentId(
            subscriptionId, resourceGroupName, applicationDeploymentName);
    logger.info("application resource id: {}", applicationResourceId);
    try {
      GenericResource applicationDeployment =
          client.genericResources().getById(applicationResourceId, apiVersion);
      logger.info("application deployment: {}", applicationDeployment);
      logger.info("application deployment properties: {}", applicationDeployment.properties());
      logger.info(
          "application deployment parameters: {}",
          ((Map<String, Map<String, Map<String, String>>>) applicationDeployment.properties())
              .get("parameters"));
      logger.info(
          "application deployment auth param key: {}",
          ((Map<String, Map<String, Map<String, String>>>) applicationDeployment.properties())
              .get("parameters")
              .get(AUTH_PARAM_KEY));

      return ((Map<String, Map<String, Map<String, String>>>) applicationDeployment.properties())
          .get("parameters")
          .get(AUTH_PARAM_KEY)
          .get("value")
          .strip()
          .equalsIgnoreCase(user.getEmail());
    } catch (Exception e) {
      logger.error("Error connecting to application", e);
      return false;
    }
  }
}
