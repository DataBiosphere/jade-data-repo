package bio.terra.service.resourcemanagement;

import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS;
import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.REVOKE_PERMISSIONS;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResourceService {

  private static final Logger logger = LoggerFactory.getLogger(ResourceService.class);
  public static final String BQ_JOB_USER_ROLE = "roles/bigquery.jobUser";

  private final AzureDataLocationSelector azureDataLocationSelector;
  private final GoogleProjectService projectService;
  private final GoogleBucketService bucketService;
  private final AzureApplicationDeploymentService applicationDeploymentService;
  private final AzureStorageAccountService storageAccountService;
  private final SamConfiguration samConfiguration;
  private final DatasetStorageAccountDao datasetStorageAccountDao;

  @Autowired
  public ResourceService(
      AzureDataLocationSelector azureDataLocationSelector,
      GoogleProjectService projectService,
      GoogleBucketService bucketService,
      AzureApplicationDeploymentService applicationDeploymentService,
      AzureStorageAccountService storageAccountService,
      SamConfiguration samConfiguration,
      DatasetStorageAccountDao datasetStorageAccountDao) {
    this.azureDataLocationSelector = azureDataLocationSelector;
    this.projectService = projectService;
    this.bucketService = bucketService;
    this.applicationDeploymentService = applicationDeploymentService;
    this.storageAccountService = storageAccountService;
    this.samConfiguration = samConfiguration;
    this.datasetStorageAccountDao = datasetStorageAccountDao;
  }

  /**
   * Fetch/create a project
   *
   * @param dataset
   * @param billingProfile authorized profile for billing account information case we need to create
   *     a project
   * @return a reference to the project as a POJO GoogleProjectResource
   */
  public GoogleProjectResource initializeProjectForBucket(
      Dataset dataset, BillingProfileModel billingProfile, String projectId)
      throws GoogleResourceException, InterruptedException {

    Map<String, String> labels =
        Map.of(
            "dataset-name", dataset.getName(),
            "dataset-id", dataset.getId().toString(),
            "project-usage", "bucket");

    final GoogleRegion region =
        (GoogleRegion)
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.FIRESTORE);
    // Every bucket needs to live in a project, so we get or create a project first
    return projectService.initializeGoogleProject(projectId, billingProfile, null, region, labels);
  }

  /**
   * Fetch/create a project, then use that to fetch/create a bucket.
   *
   * @param flightId used to lock the bucket metadata during possible creation
   * @return a reference to the bucket as a POJO GoogleBucketResource
   * @throws CorruptMetadataException in two cases.
   *     <ul>
   *       <li>if the bucket already exists, but the metadata does not AND the application property
   *           allowReuseExistingBuckets=false.
   *       <li>if the metadata exists, but the bucket does not
   *     </ul>
   */
  public GoogleBucketResource getOrCreateBucketForFile(
      Dataset dataset, GoogleProjectResource projectResource, String flightId)
      throws InterruptedException, GoogleResourceNamingException {
    return bucketService.getOrCreateBucket(
        projectService.bucketForFile(projectResource.getGoogleProjectId()),
        projectResource,
        (GoogleRegion)
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET),
        flightId);
  }

  /**
   * Given an application deployment, get or create a storage account.
   *
   * @param dataset dataset to create storage account for
   * @param billingProfile authorized profile for billing account information case we need to create
   *     an application deployment or storage account
   * @param flightId used to lock the bucket metadata during possible creation
   * @return a reference to the bucket as a POJO AzureStorageAccountResource
   * @throws CorruptMetadataException in two cases.
   *     <ul>
   *       <li>if the storage account already exists, but the metadata does not AND the application
   *           property allowReuseExistingBuckets=false.
   *       <li>if the metadata exists, but the storage account does not
   *     </ul>
   */
  public AzureStorageAccountResource getOrCreateStorageAccount(
      Dataset dataset, BillingProfileModel billingProfile, String flightId)
      throws InterruptedException {
    final AzureRegion region =
        (AzureRegion)
            dataset
                .getDatasetSummary()
                .getStorageResourceRegion(AzureCloudResource.STORAGE_ACCOUNT);
    // Every storage account needs to live in a deployed application's managed resource group, so we
    // make sure that
    // the application deployment is registered first
    final AzureApplicationDeploymentResource applicationResource =
        applicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile);

    List<UUID> storageAccountsForDataset =
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId());

    String storageAccountName =
        azureDataLocationSelector.storageAccountNameForDataset(
            applicationResource.getStorageAccountPrefix(), dataset.getName(), billingProfile);

    List<AzureStorageAccountResource> storageAccountsForBillingProfile =
        storageAccountsForDataset.stream()
            .map(this::lookupStorageAccount)
            .filter(a -> storageAccountIsForBillingProfile(a, billingProfile))
            .collect(Collectors.toList());

    // there should never be more than one storage account per dataset / billing profile,
    // so we just take the first one
    if (storageAccountsForBillingProfile.size() > 0) {
      AzureStorageAccountResource storageAccount = storageAccountsForBillingProfile.get(0);
      storageAccountName = storageAccount.getName();

      if (storageAccountsForBillingProfile.size() > 1) {
        logger.warn(
            "Found more than one storage account associated with this dataset and billing profile");
      }
    }

    return storageAccountService.getOrCreateStorageAccount(
        storageAccountName, applicationResource, region, flightId);
  }

  /**
   * Delete the metadata and cloud storage account. Note: this will not check references and delete
   * the storage even if it contains data
   *
   * @param dataset The dataset whose storage account to delete
   * @param flightId The flight that might potentially have the storage account locked
   */
  public void deleteStorageAccount(Dataset dataset, String flightId) {
    // Get list of linked accounts
    List<UUID> sasToDelete =
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId());

    sasToDelete.forEach(
        s -> {
          logger.info("Deleting storage account id {}", s);
          AzureStorageAccountResource storageAccountResource =
              storageAccountService.retrieveStorageAccountById(s);
          storageAccountService.deleteCloudStorageAccount(storageAccountResource, flightId);
        });
  }

  private boolean storageAccountIsForBillingProfile(
      AzureStorageAccountResource storageAccount, BillingProfileModel billingProfile) {
    AzureApplicationDeploymentResource resource = storageAccount.getApplicationResource();
    return resource.getProfileId().equals(billingProfile.getId());
  }

  /**
   * Fetch an existing bucket and check that the associated cloud resource exists.
   *
   * @param bucketResourceId our identifier for the bucket
   * @return a reference to the bucket as a POJO GoogleBucketResource
   * @throws GoogleResourceNotFoundException if the bucket_resource metadata row does not exist
   * @throws CorruptMetadataException if the bucket_resource metadata row exists but the cloud
   *     resource does not
   */
  public GoogleBucketResource lookupBucket(String bucketResourceId) {
    return lookupBucket(UUID.fromString(bucketResourceId));
  }

  public GoogleBucketResource lookupBucket(UUID bucketResourceId) {
    return bucketService.getBucketResourceById(bucketResourceId, true);
  }

  public AzureStorageAccountResource lookupStorageAccount(UUID storageAccountResourceId) {
    return storageAccountService.getStorageAccountResourceById(storageAccountResourceId, true);
  }

  /**
   * Fetch an existing bucket_resource metadata row. Note this method does not check for the
   * existence of the underlying cloud resource. This method is intended for places where an
   * existence check on the associated cloud resource might be too much overhead (e.g. DRS lookups).
   * Most bucket lookups should use the lookupBucket method instead, which has additional overhead
   * but will catch metadata corruption errors sooner.
   *
   * @param bucketResourceId our identifier for the bucket
   * @return a reference to the bucket as a POJO GoogleBucketResource
   * @throws GoogleResourceNotFoundException if the bucket_resource metadata row does not exist
   */
  public GoogleBucketResource lookupBucketMetadata(String bucketResourceId) {
    return bucketService.getBucketResourceById(UUID.fromString(bucketResourceId), false);
  }

  /**
   * Update the bucket_resource metadata table to match the state of the underlying cloud. - If the
   * bucket exists, then the metadata row should also exist and be unlocked. - If the bucket does
   * not exist, then the metadata row should not exist. If the metadata row is locked, then only the
   * locking flight can unlock or delete the row.
   *
   * @param projectId retrieve bucket based on google project id
   * @param billingProfile an authorized billing profile
   * @param flightId flight doing the updating
   */
  public void updateBucketMetadata(
      String projectId, BillingProfileModel billingProfile, String flightId)
      throws GoogleResourceNamingException {
    String bucketName = projectService.bucketForFile(projectId);
    bucketService.updateBucketMetadata(bucketName, flightId);
  }

  /**
   * Create a new project for a snapshot, if none exists already.
   *
   * @param billingProfile authorized billing profile to pay for the project
   * @param region the region to create the Firestore in
   * @return project resource id
   */
  public UUID initializeSnapshotProject(
      BillingProfileModel billingProfile,
      String projectId,
      GoogleRegion region,
      List<Dataset> sourceDatasets,
      String snapshotName,
      UUID snapshotId)
      throws InterruptedException {

    String datasetNames =
        sourceDatasets.stream().map(Dataset::getName).collect(Collectors.joining(","));

    String datasetIds =
        sourceDatasets.stream()
            .map(Dataset::getId)
            .map(UUID::toString)
            .collect(Collectors.joining(","));

    Map<String, String> labels =
        Map.of(
            "dataset-names", datasetNames,
            "dataset-ids", datasetIds,
            "snapshot-name", snapshotName,
            "snapshot-id", snapshotId.toString(),
            "project-usage", "snapshot");

    GoogleProjectResource googleProjectResource =
        projectService.initializeGoogleProject(projectId, billingProfile, null, region, labels);

    return googleProjectResource.getId();
  }

  /**
   * Create a new project for a dataset, if none exists already.
   *
   * @param billingProfile authorized billing profile to pay for the project
   * @param region the region to create the project in
   * @return project resource id
   */
  public UUID getOrCreateDatasetProject(
      BillingProfileModel billingProfile,
      String projectId,
      GoogleRegion region,
      String datasetName,
      UUID datasetId,
      Boolean isAzure)
      throws InterruptedException {

    Map<String, String> labels = new HashMap<>();
    labels.put("dataset-name", datasetName);
    labels.put("dataset-id", datasetId.toString());
    labels.put("project-usage", "dataset");

    if (isAzure) {
      labels.put("is-azure", "true");
    }

    GoogleProjectResource googleProjectResource =
        projectService.initializeGoogleProject(
            projectId, billingProfile, getStewardPolicy(), region, labels);

    return googleProjectResource.getId();
  }

  /**
   * Look up an existing project resource given its id
   *
   * @param projectResourceId unique id for the project
   * @return project resource
   */
  public GoogleProjectResource getProjectResource(UUID projectResourceId) {
    return projectService.getProjectResourceById(projectResourceId);
  }

  /**
   * Look up an existing application deployment resource given its id
   *
   * @param applicationId unique id for the application deployment
   * @return application deployment resource
   */
  public AzureApplicationDeploymentResource getApplicationDeploymentResource(UUID applicationId) {
    return applicationDeploymentService.getApplicationDeploymentResourceById(applicationId);
  }

  public void grantPoliciesBqJobUser(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    final List<String> emails =
        policyEmails.stream().map((e) -> "group:" + e).collect(Collectors.toList());
    projectService.updateIamPermissions(
        Collections.singletonMap(BQ_JOB_USER_ROLE, emails), dataProject, ENABLE_PERMISSIONS);
  }

  public void revokePoliciesBqJobUser(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    final List<String> emails =
        policyEmails.stream().map((e) -> "group:" + e).collect(Collectors.toList());
    projectService.updateIamPermissions(
        Collections.singletonMap(BQ_JOB_USER_ROLE, emails), dataProject, REVOKE_PERMISSIONS);
  }

  private Map<String, List<String>> getStewardPolicy() {
    // get steward emails and add to policy
    String stewardsGroupEmail = "group:" + samConfiguration.getStewardsGroupEmail();
    Map<String, List<String>> policyMap = new HashMap<>();
    policyMap.put(BQ_JOB_USER_ROLE, Collections.singletonList(stewardsGroupEmail));
    return Collections.unmodifiableMap(policyMap);
  }

  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    return projectService.markUnusedProjectsForDelete(profileId);
  }

  public List<UUID> markUnusedApplicationDeploymentsForDelete(UUID profileId) {
    return applicationDeploymentService.markUnusedApplicationDeploymentsForDelete(profileId);
  }

  public void deleteUnusedProjects(List<UUID> projectIdList) {
    projectService.deleteUnusedProjects(projectIdList);
  }

  public void deleteProjectMetadata(List<UUID> projectIdList) {
    projectService.deleteProjectMetadata(projectIdList);
  }

  public void deleteDeployedApplicationMetadata(List<UUID> applicationIdList) {
    applicationDeploymentService.deleteApplicationDeploymentMetadata(applicationIdList);
  }
}
