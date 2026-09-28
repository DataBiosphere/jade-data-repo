package bio.terra.service.resourcemanagement;

import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS;
import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.REVOKE_PERMISSIONS;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.CollectionType;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  public static final List<String> SNAPSHOT_GCP_IAM_ROLES =
      List.of(BQ_JOB_USER_ROLE, SERVICE_USAGE_CONSUMER_ROLE);

  private final GoogleProjectService projectService;
  private final GoogleBucketService bucketService;
  private final SamConfiguration samConfiguration;
  private final GoogleResourceManagerService resourceManagerService;
  private final ProfileDao profileDao;

  @Autowired
  public ResourceService(
      GoogleProjectService projectService,
      GoogleBucketService bucketService,
      SamConfiguration samConfiguration,
      GoogleResourceManagerService resourceManagerService,
      ProfileDao profileDao) {
    this.projectService = projectService;
    this.bucketService = bucketService;
    this.samConfiguration = samConfiguration;
    this.resourceManagerService = resourceManagerService;
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
        projectId, billingProfile, region, labels, CollectionType.DATASET);
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
   * @return project resource id
   */
  public UUID initializeSnapshotProject(
      BillingProfileModel billingProfile,
      String projectId,
      Dataset sourceDataset,
      String snapshotName,
      UUID snapshotId)
      throws InterruptedException {

    GoogleRegion region =
        (GoogleRegion)
            sourceDataset
                .getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.FIRESTORE);

    Map<String, String> labels =
        Map.of(
            "dataset-names", sourceDataset.getName(),
            "dataset-ids", sourceDataset.getId().toString(),
            "snapshot-name", snapshotName,
            "snapshot-id", snapshotId.toString(),
            "project-usage", "snapshot");

    GoogleProjectResource googleProjectResource =
        projectService.initializeGoogleProject(
            projectId, billingProfile, region, labels, CollectionType.SNAPSHOT);

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
            projectId, billingProfile, region, labels, CollectionType.DATASET);

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
  public void assignRolesForSnapshot(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    modifyRoles(dataProject, policyEmails, SNAPSHOT_GCP_IAM_ROLES, ENABLE_PERMISSIONS);
  }

  public void revokeRolesForSnapshot(String dataProject, Collection<String> policyEmails)
      throws InterruptedException {
    modifyRoles(dataProject, policyEmails, SNAPSHOT_GCP_IAM_ROLES, REVOKE_PERMISSIONS);
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

  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    return projectService.markUnusedProjectsForDelete(profileId);
  }

  public List<UUID> markUnusedProjectsForDelete(List<UUID> projectResourceIds) {
    return projectService.markUnusedProjectsForDelete(projectResourceIds);
  }

  public void deleteUnusedProjects(List<UUID> projectIdList) {
    projectService.deleteUnusedProjects(projectIdList);
  }

  public void deleteProjectMetadata(List<UUID> projectIdList) {
    projectService.deleteProjectMetadata(projectIdList);
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
