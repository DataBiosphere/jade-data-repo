package bio.terra.service.resourcemanagement.azure;

import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.AzureStorageAccountSkuType;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.MismatchedBillingProfilesException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.resources.models.GenericResource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureApplicationDeploymentService {
  private static final Logger logger =
      LoggerFactory.getLogger(AzureApplicationDeploymentService.class);

  /** Azure specific keys */
  static final String MANAGED_RESOURCE_GROUP_ID_KEY = "managedResourceGroupId";

  static final String PARAMETERS_KEY = "parameters";
  static final String PARAMETER_VALUE_KEY = "value";

  /** TDR app deployment specific keys */
  static final String DEFAULT_REGION_KEY = "location";

  static final String STORAGE_PREFIX_KEY = "storageAccountNamePrefix";
  static final String STORAGE_TYPE_KEY = "storageAccountType";
  static final String AUTHORIZED_TDR_USER = "authorizedTDRUser";

  private final AzureResourceDao resourceDao;
  private final AzureResourceConfiguration resourceConfiguration;
  private final ObjectMapper objectMapper;

  @Autowired
  public AzureApplicationDeploymentService(
      AzureResourceDao resourceDao,
      AzureResourceConfiguration resourceConfiguration,
      ObjectMapper objectMapper) {
    this.resourceDao = resourceDao;
    this.resourceConfiguration = resourceConfiguration;
    this.objectMapper = objectMapper;
  }

  /**
   * Return the Azure application deployment associated with the {@link BillingProfileModel}, first
   * registering it in Postgres if it doesn't yet exist.
   *
   * @param billingProfile previously authorized billing profile
   * @return the {@link AzureApplicationDeploymentResource} associated with the billing profile
   * @throws MismatchedBillingProfilesException if another billing profile has already registered
   *     its association with the Azure application deployment.
   */
  public AzureApplicationDeploymentResource getOrRegisterApplicationDeployment(
      BillingProfileModel billingProfile) {

    try {
      AzureApplicationDeploymentResource applicationResource =
          resourceDao.retrieveApplicationDeploymentByName(
              billingProfile.getApplicationDeploymentName());
      if (Objects.equals(applicationResource.getProfileId(), billingProfile.getId())) {
        return applicationResource;
      }
      throw new MismatchedBillingProfilesException(
          "Cannot reuse existing application deployment "
              + applicationResource.getAzureApplicationDeploymentName()
              + " from profile "
              + applicationResource.getProfileId()
              + " with a different profile "
              + billingProfile.getId());
    } catch (AzureResourceNotFoundException e) {
      logger.info(
          "no application deployment resource found for application name: {}",
          billingProfile.getApplicationDeploymentName());
    }

    return newApplicationDeployment(billingProfile);
  }

  public AzureApplicationDeploymentResource getApplicationDeploymentResourceById(UUID id) {
    return resourceDao.retrieveApplicationDeploymentById(id);
  }

  public List<UUID> markUnusedApplicationDeploymentsForDelete(UUID profileId) {
    return resourceDao.markUnusedApplicationDeploymentsForDelete(profileId);
  }

  public void deleteApplicationDeploymentMetadata(List<UUID> applicationIds) {
    resourceDao.deleteApplicationDeploymentMetadata(applicationIds);
  }

  /**
   * Return the the application deployment associated with the specified {@link BillingProfileModel}
   *
   * @param billingProfile The billing profile model to use to connect to and read the managed
   *     metadata for
   * @return A {@link GenericResource} object
   * @throws AzureResourceNotFoundException if the resource isn't found
   */
  public GenericResource retrieveApplicationDeployment(BillingProfileModel billingProfile) {
    AzureResourceManager client =
        resourceConfiguration.getClient(billingProfile.getSubscriptionId());

    logger.info("Looking up application");
    String applicationResourceId =
        MetadataDataAccessUtils.getApplicationDeploymentId(billingProfile);
    return client
        .genericResources()
        .getById(applicationResourceId, resourceConfiguration.apiVersion());
  }

  /**
   * Return the input parameters for the application deployment associated with the specified {@link
   * BillingProfileModel}
   *
   * @param applicationDeployment he Azure application resource to read metadata for
   * @return A String, String Map of input values for the application deployment
   * @throws AzureResourceNotFoundException if the resource isn't found
   */
  public Map<String, String> inputParameters(GenericResource applicationDeployment) {
    JsonNode properties = objectMapper.valueToTree(applicationDeployment.properties());

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                properties.get(PARAMETERS_KEY).fields(), Spliterator.ORDERED),
            false)
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> e.getValue().get(PARAMETER_VALUE_KEY).asText()));
  }

  /**
   * Register and return the Azure application deployment associated with the {@link
   * BillingProfileModel}.
   *
   * @param billingProfile previously authorized billing profile
   * @return the {@link AzureApplicationDeploymentResource} associated with the billing profile
   */
  private AzureApplicationDeploymentResource newApplicationDeployment(
      BillingProfileModel billingProfile) {
    var applicationDeployment = retrieveApplicationDeployment(billingProfile);

    // TODO: move to output of template
    var properties = objectMapper.valueToTree(applicationDeployment.properties());
    var managedResourceGroupId = properties.get(MANAGED_RESOURCE_GROUP_ID_KEY).asText();
    var managedResourceGroupPathParts = managedResourceGroupId.split("/");
    var managedResourceGroupName =
        managedResourceGroupPathParts[managedResourceGroupPathParts.length - 1];

    var parameters = inputParameters(applicationDeployment);

    // TODO: should we store version?
    var resource =
        new AzureApplicationDeploymentResource()
            .profileId(billingProfile.getId())
            .azureApplicationDeploymentId(applicationDeployment.id())
            .azureApplicationDeploymentName(billingProfile.getApplicationDeploymentName())
            .azureResourceGroupName(managedResourceGroupName)
            .azureSynapseWorkspaceName("N/A")
            .defaultRegion(AzureRegion.fromValue(parameters.get(DEFAULT_REGION_KEY)))
            .storageAccountPrefix(parameters.get(STORAGE_PREFIX_KEY))
            .storageAccountSkuType(
                AzureStorageAccountSkuType.fromAzureName(parameters.get(STORAGE_TYPE_KEY)));

    var applicationDeploymentId = resourceDao.createApplicationDeployment(resource);
    return resource.id(applicationDeploymentId);
  }
}
