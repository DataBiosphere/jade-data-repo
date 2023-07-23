package bio.terra.service.resourcemanagement;

import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS;
import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.REVOKE_PERMISSIONS;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CollectionType;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.exception.StorageResourceNotFoundException;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.resourcemanagement.exception.AzureResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotStorageAccountDao;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResourceService {

  private static final Logger logger = LoggerFactory.getLogger(ResourceService.class);
  public static final String BQ_JOB_USER_ROLE = "roles/bigquery.jobUser";
  public static final String SERVICE_USAGE_CONSUMER_ROLE =
      "roles/serviceusage.serviceUsageConsumer";

  private final AzureDataLocationSelector azureDataLocationSelector;
  private final GoogleProjectService projectService;
  private final GoogleBucketService bucketService;
  private final AzureApplicationDeploymentService applicationDeploymentService;
  private final AzureStorageAccountService storageAccountService;
  private final SamConfiguration samConfiguration;
  private final DatasetStorageAccountDao datasetStorageAccountDao;
  private final SnapshotStorageAccountDao snapshotStorageAccountDao;
  private final GoogleResourceManagerService resourceManagerService;
  private final AzureContainerPdao azureContainerPdao;
  private final ProfileDao profileDao;

  @Autowired
  public ResourceService(
      AzureDataLocationSelector azureDataLocationSelector,
      GoogleProjectService projectService,
      GoogleBucketService bucketService,
      AzureApplicationDeploymentService applicationDeploymentService,
      AzureStorageAccountService storageAccountService,
      SamConfiguration samConfiguration,
      DatasetStorageAccountDao datasetStorageAccountDao,
      SnapshotStorageAccountDao snapshotStorageAccountDao,
      GoogleResourceManagerService resourceManagerService,
      AzureContainerPdao azureContainerPdao,
      ProfileDao profileDao) {
    this.azureDataLocationSelector = azureDataLocationSelector;
    this.projectService = projectService;
    this.bucketService = bucketService;
    this.applicationDeploymentService = applicationDeploymentService;
    this.storageAccountService = storageAccountService;
    this.samConfiguration = samConfiguration;
    this.datasetStorageAccountDao = datasetStorageAccountDao;
    this.snapshotStorageAccountDao = snapshotStorageAccountDao;
    this.resourceManagerService = resourceManagerService;
    this.azureContainerPdao = azureContainerPdao;
    this.profileDao = profileDao;
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
    return projectService.initializeGoogleProject(
        projectId, billingProfile, null, region, labels, CollectionType.DATASET);
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
      Dataset dataset,
      GoogleProjectResource projectResource,
      String flightId,
      Callable<List<String>> getReaderGroups)
      throws InterruptedException, GoogleResourceNamingException {
    return getOrCreateBucketForFile(
        (GoogleRegion)
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET),
        projectResource,
        flightId,
        getReaderGroups,
        dataset.getProjectResource().getServiceAccount());
  }

  /**
   * Fetch/create a project, then use that to fetch/create a bucket.
   *
   * <p>Autoclass will be enabled on the bucket by default
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
      GoogleRegion region,
      GoogleProjectResource projectResource,
      String flightId,
      Callable<List<String>> getReaderGroups,
      String dedicatedServiceAccount)
      throws InterruptedException, GoogleResourceNamingException {
    return bucketService.getOrCreateBucket(
        projectService.bucketForFile(projectResource.getGoogleProjectId()),
        projectResource,
        region,
        flightId,
        null,
        getReaderGroups,
        dedicatedServiceAccount,
        true);
  }

  /**
   * Get or create a bucket for the ingest scratch files
   *
   * <p>Autoclass is disabled by default on scratch file buckets Cost of Autoclass would most likely
   * outweigh savings due to usage pattern of files in scratch file buckets
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
  public GoogleBucketResource getOrCreateBucketForBigQueryScratchFile(
      Dataset dataset, String flightId) throws InterruptedException, GoogleResourceNamingException {
    GoogleProjectResource projectResource = dataset.getProjectResource();
    return bucketService.getOrCreateBucket(
        projectService.bucketForBigQueryScratchFile(projectResource.getGoogleProjectId()),
        projectResource,
        (GoogleRegion)
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BIGQUERY),
        flightId,
        null,
        null,
        dataset.getProjectResource().getServiceAccount(),
        false);
  }

  /**
   * Get or create a bucket for snapshot export files
   *
   * <p>Autoclass disabled by default for snapshot export buckets Cost of Autoclass would most
   * likely outweigh savings due to usage pattern of files in snapshot export buckets
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
  public GoogleBucketResource getOrCreateBucketForSnapshotExport(Snapshot snapshot, String flightId)
      throws InterruptedException, GoogleResourceNamingException {
    GoogleProjectResource projectResource = snapshot.getProjectResource();
    return bucketService.getOrCreateBucket(
        projectService.bucketForSnapshotExport(projectResource.getGoogleProjectId()),
        projectResource,
        (GoogleRegion)
            snapshot
                .getFirstSnapshotSource()
                .getDataset()
                .getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.BIGQUERY),
        flightId,
        Duration.ofDays(1),
        null,
        null,
        false);
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
  public AzureStorageAccountResource getOrCreateDatasetStorageAccount(
      Dataset dataset, BillingProfileModel billingProfile, String flightId)
      throws InterruptedException {
    final AzureRegion region = dataset.getStorageAccountRegion();

    // Every storage account needs to live in a deployed application's managed resource group, so we
    // make sure that
    // the application deployment is registered first
    final AzureApplicationDeploymentResource applicationResource =
        applicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile);

    String storageAccountName =
        getDatasetStorageAccount(dataset.getId(), billingProfile)
            .map(AzureStorageAccountResource::getName)
            .orElse(
                azureDataLocationSelector.createStorageAccountName(
                    applicationResource.getStorageAccountPrefix(),
                    dataset.getStorageAccountRegion(),
                    billingProfile,
                    dataset.isSecureMonitoringEnabled()));

    return storageAccountService.getOrCreateStorageAccount(
        storageAccountName, dataset.getId().toString(), applicationResource, region, flightId);
  }

  public AzureStorageAccountResource createSnapshotStorageAccount(
      UUID snapshotId,
      AzureRegion datasetAzureRegion,
      BillingProfileModel billingProfile,
      String flightId,
      boolean isSecureMonitoringEnabled)
      throws InterruptedException {

    final AzureApplicationDeploymentResource applicationResource =
        applicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile);
    String computedStorageAccountName =
        azureDataLocationSelector.createStorageAccountName(
            applicationResource.getStorageAccountPrefix(),
            datasetAzureRegion,
            billingProfile,
            isSecureMonitoringEnabled);

    AzureStorageAccountResource storageAccountResource =
        storageAccountService.getOrCreateStorageAccount(
            computedStorageAccountName,
            snapshotId.toString(),
            applicationResource,
            datasetAzureRegion,
            flightId);

    snapshotStorageAccountDao.createSnapshotStorageAccountLink(
        snapshotId, storageAccountResource.getResourceId());

    return storageAccountResource;
  }

  /**
   * Get a storage account for a dataset/billing profile combo.
   *
   * @param dataset dataset to get storage account for
   * @param billingProfile billing profile to get storage account for.
   * @return Optional AzureStorageAccountResource
   */
  public AzureStorageAccountResource getDatasetStorageAccount(
      Dataset dataset, BillingProfileModel billingProfile) {
    return getDatasetStorageAccount(dataset.getId(), billingProfile)
        .orElseThrow(
            () ->
                new CorruptMetadataException(
                    String.format(
                        "No storage account resource for dataset %s and billing profile %s",
                        dataset.getId(), dataset.getDefaultProfileId())));
  }

  private Optional<AzureStorageAccountResource> getDatasetStorageAccount(
      UUID datasetId, BillingProfileModel billingProfile) {
    var storageAccountResourceIds =
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(datasetId);
    var storageAccountResources =
        storageAccountResourceIds.stream()
            .map(this::lookupStorageAccount)
            .filter(sa -> storageAccountIsForBillingProfile(sa, billingProfile))
            .collect(Collectors.toList());

    Optional<AzureStorageAccountResource> storageAccountResource;
    switch (storageAccountResources.size()) {
      case 0:
        storageAccountResource = Optional.empty();
        break;
      case 1:
        storageAccountResource = Optional.of(storageAccountResources.get(0));
        break;
      default:
        throw new CorruptMetadataException(
            "Dataset/Billing Profile combination has more than 1 associated storage account");
    }
    return storageAccountResource;
  }

  public AzureStorageAccountResource getSnapshotStorageAccount(UUID snapshotId) {
    return snapshotStorageAccountDao
        .getStorageAccountResourceIdForSnapshotId(snapshotId)
        .map(this::lookupStorageAccount)
        .orElseThrow(
            () ->
                new StorageResourceNotFoundException(
                    "Snapshot storage account was not found"));
  }

  public AzureStorageAuthInfo getDatasetStorageAuthInfo(Dataset dataset) {
    BillingProfileModel billingProfileModel =
        profileDao.getBillingProfileById(dataset.getDefaultProfileId());
    AzureStorageAccountResource storageAccountResource =
        getDatasetStorageAccount(dataset, billingProfileModel);
    return AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
        billingProfileModel, storageAccountResource);
  }

  public AzureStorageAuthInfo getSnapshotStorageAuthInfo(Snapshot snapshot) {
    return getSnapshotStorageAuthInfo(snapshot.getProfileId(), snapshot.getId());
  }

  public AzureStorageAuthInfo getSnapshotStorageAuthInfo(UUID billingProfileId, UUID snapshotId) {
    BillingProfileModel billingProfileModel =
        profileDao.getBillingProfileById(billingProfileId);
    AzureStorageAccountResource storageAccountResource =
        getSnapshotStorageAccount(snapshotId);
    return AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
        billingProfileModel, storageAccountResource);
  }

  /**
   * Delete the metadata and cloud storage container. Note: this will not check references and
   * delete the storage even if it contains data
   *
   * @param dataset The dataset whose storage account to delete
   * @param flightId The flight that might potentially have the storage account locked
   */
  public void deleteStorageContainer(Dataset dataset, String flightId) {
    // Get list of linked accounts
    List<UUID> sasToDelete =
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId());

    sasToDelete.forEach(
        s -> {
          logger.info("Deleting dataset storage account and container id {}", s);
          AzureStorageAccountResource storageAccountResource =
              storageAccountService.retrieveStorageAccountById(s);
          azureContainerPdao.deleteContainer(
              dataset.getDatasetSummary().getDefaultBillingProfile(), storageAccountResource);
          storageAccountService.deleteCloudStorageAccountMetadata(
              storageAccountResource.getName(),
              storageAccountResource.getTopLevelContainer(),
              flightId);
        });
  }

  /**
   * Delete the metadata and cloud storage container. Note: this will not check references and
   * delete the storage even if it contains data
   *
   * @param storageResourceId The id of the snapshot's storage account. When this gets called, tha
   *     snapshot has already been deleted so we can't look up this id in the db
   * @param profileId The id of the snapshot's billing profile
   * @param flightId The flight that might potentially have the storage account locked
   */
  public void deleteStorageContainer(UUID storageResourceId, UUID profileId, String flightId) {
    logger.info("Deleting snapshot storage account id {}", storageResourceId);
    BillingProfileModel snapshotBillingProfile = profileDao.getBillingProfileById(profileId);
    // get container
    AzureStorageAccountResource storageAccountResource =
        storageAccountService.retrieveStorageAccountById(storageResourceId);
    azureContainerPdao.deleteContainer(snapshotBillingProfile, storageAccountResource);

    storageAccountService.deleteCloudStorageAccountMetadata(
        storageAccountResource.getName(), storageAccountResource.getTopLevelContainer(), flightId);
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
   * Fetch an existing storage_account metadata row. Note this method does not check for the
   * existence of the underlying cloud resource. This method is intended for places where an
   * existence check on the associated cloud resource might be too much overhead (e.g. DRS lookups).
   * Most storage account lookups should use the lookupStorageAccount method instead, which has
   * additional overhead but will catch metadata corruption errors sooner.
   *
   * @param storageAccountId our identifier for the storage account
   * @return a reference to the storage account as a POJO AzureStorageAccountResource
   * @throws AzureResourceNotFoundException if the storage_account metadata row does not exist
   */
  public AzureStorageAccountResource lookupStorageAccountMetadata(String storageAccountId) {
    return storageAccountService.getStorageAccountResourceById(
        UUID.fromString(storageAccountId), false);
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
   * @return project resource id
   */
  public UUID initializeSnapshotProject(
      BillingProfileModel billingProfile,
      String projectId,
      List<Dataset> sourceDatasets,
      String snapshotName,
      UUID snapshotId)
      throws InterruptedException {

    GoogleRegion region =
        (GoogleRegion)
            sourceDatasets
                .iterator()
                .next()
                .getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.FIRESTORE);

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
        projectService.initializeGoogleProject(
            projectId, billingProfile, null, region, labels, CollectionType.SNAPSHOT);

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
      UUID datasetId)
      throws InterruptedException {

    Map<String, String> labels = new HashMap<>();
    labels.put("dataset-name", datasetName);
    labels.put("dataset-id", datasetId.toString());
    labels.put("project-usage", "dataset");

    GoogleProjectResource googleProjectResource =
        projectService.initializeGoogleProject(
            projectId, billingProfile, getStewardPolicy(), region, labels, CollectionType.DATASET);

    return googleProjectResource.getId();
  }

  /**
   * Create a new service account for a dataset to be used to ingest.
   *
   * @param projectId the Google id of the project to create the SA for
   * @param datasetName the name of the dataset the SA is being created for
   * @return email of the created service account
   */
  public String createDatasetServiceAccount(String projectId, String datasetName) {

    return projectService.createProjectServiceAccount(
        projectId, CollectionType.DATASET, datasetName);
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
    modifyRoles(dataProject, policyEmails, List.of(BQ_JOB_USER_ROLE), ENABLE_PERMISSIONS);
  }

  public void revokePoliciesBqJobUser(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    modifyRoles(dataProject, policyEmails, List.of(BQ_JOB_USER_ROLE), REVOKE_PERMISSIONS);
  }

  public void grantPoliciesServiceUsageConsumer(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    modifyRoles(
        dataProject, policyEmails, List.of(SERVICE_USAGE_CONSUMER_ROLE), ENABLE_PERMISSIONS);
  }

  public void revokePoliciesServiceUsageConsumer(
      String dataProject, Collection<String> policyEmails) throws InterruptedException {
    modifyRoles(
        dataProject, policyEmails, List.of(SERVICE_USAGE_CONSUMER_ROLE), REVOKE_PERMISSIONS);
  }

  private void modifyRoles(
      String dataProject,
      Collection<String> policyEmails,
      List<String> roles,
      GoogleProjectService.PermissionOp op)
      throws InterruptedException {
    final List<String> emails =
        policyEmails.stream().map(ResourceService::formatEmailForPolicy).toList();
    final Map<String, List<String>> userPermissions =
        roles.stream().collect(Collectors.toMap(r -> r, r -> emails));
    resourceManagerService.updateIamPermissions(userPermissions, dataProject, op);
  }

  private Map<String, List<String>> getStewardPolicy() {
    // get steward emails and add to policy
    String stewardsGroupEmail = formatEmailForPolicy(samConfiguration.getStewardsGroupEmail());
    Map<String, List<String>> policyMap = new HashMap<>();
    policyMap.put(BQ_JOB_USER_ROLE, Collections.singletonList(stewardsGroupEmail));
    return Collections.unmodifiableMap(policyMap);
  }

  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    return projectService.markUnusedProjectsForDelete(profileId);
  }

  public List<UUID> markUnusedProjectsForDelete(List<UUID> projectResourceIds) {
    return projectService.markUnusedProjectsForDelete(projectResourceIds);
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

  public static String formatEmailForPolicy(String email) {
    if (email == null) {
      return null;
    }
    if (email.endsWith(".iam.gserviceaccount.com")) {
      return "serviceAccount:" + email;
    } else {
      return "group:" + email;
    }
  }
}
