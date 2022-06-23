package bio.terra.service.resourcemanagement.google;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.CollectionType;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.exception.AppengineException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.MismatchedBillingProfilesException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.Application;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Operation;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.google.api.services.iam.v1.model.CreateServiceAccountRequest;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.api.services.serviceusage.v1.ServiceUsage;
import com.google.api.services.serviceusage.v1.model.BatchEnableServicesRequest;
import com.google.api.services.serviceusage.v1.model.GoogleApiServiceusageV1Service;
import com.google.api.services.serviceusage.v1.model.ListServicesResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GoogleProjectService {
  private static final Logger logger = LoggerFactory.getLogger(GoogleProjectService.class);
  private static final String ENABLED_FILTER = "state:ENABLED";
  private static final List<String> DATA_PROJECT_SERVICE_IDS =
      Collections.unmodifiableList(
          Arrays.asList(
              "bigquery-json.googleapis.com",
              "firestore.googleapis.com",
              "firebaserules.googleapis.com",
              "storage-component.googleapis.com",
              "storage-api.googleapis.com",
              "cloudbilling.googleapis.com",
              "appengine.googleapis.com"));

  /** This is where the Firestore database will be created. */
  private static final String FIRESTORE_DB_TYPE = "CLOUD_FIRESTORE";

  /** The name of the project-specific service account to create. */
  private static final String SERVICE_ACCOUNT_NAME = "tdr-ingest-sa";

  private final GoogleBillingService billingService;
  private final GoogleResourceDao resourceDao;
  private final GoogleResourceConfiguration resourceConfiguration;
  private final GoogleResourceManagerService resourceManagerService;
  private final BufferService bufferService;
  private final DatasetBucketDao datasetBucketDao;

  private static final String GS_BUCKET_PATTERN = "[a-z0-9\\-\\.\\_]{3,63}";

  @Autowired
  public GoogleProjectService(
      GoogleResourceDao resourceDao,
      GoogleResourceConfiguration resourceConfiguration,
      GoogleBillingService billingService,
      GoogleResourceManagerService resourceManagerService,
      BufferService bufferService,
      DatasetBucketDao datasetBucketDao) {
    this.resourceDao = resourceDao;
    this.resourceConfiguration = resourceConfiguration;
    this.billingService = billingService;
    this.resourceManagerService = resourceManagerService;
    this.bufferService = bufferService;
    this.datasetBucketDao = datasetBucketDao;
  }

  public String bucketForBigQueryScratchFile(String googleProjectId) {
    return googleProjectId + "-bq-scratch-bucket";
  }

  public String bucketForSnapshotExport(String googleProjectId) {
    return googleProjectId + "-snapshot-export-bucket";
  }

  /**
   * @param dataset Dataset the file belongs to
   * @param billingProfile
   * @return
   * @throws GoogleResourceException
   */
  public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile)
      throws GoogleResourceException {
    // Case 1
    // Condition: Requested billing profile matches source dataset's billing profile
    // Action: Re-use dataset's project
    String sourceDatasetGoogleProjectId = dataset.getProjectResource().getGoogleProjectId();
    UUID sourceDatasetBillingProfileId = dataset.getProjectResource().getProfileId();
    UUID requestedBillingProfileId = billingProfile.getId();
    if (sourceDatasetBillingProfileId.equals(requestedBillingProfileId)) {
      return sourceDatasetGoogleProjectId;
    }

    // Case 2
    // Condition: Ingest Billing profile != source dataset billing profile && project *already
    // exists*
    // Action: Re-use bucket's project
    String bucketGoogleProjectId =
        datasetBucketDao.getProjectResourceForBucket(dataset.getId(), billingProfile.getId());
    if (bucketGoogleProjectId != null) {
      return bucketGoogleProjectId;
    }

    // Case 3 -
    // Condition: Ingest Billing profile != source dataset billing profile && project does NOT exist
    // Action: Request a new project
    ResourceInfo resource = bufferService.handoutResource(dataset.isSecureMonitoringEnabled());
    return resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
  }

  public String bucketForFile(String projectId) throws GoogleResourceNamingException {
    String bucketName = projectId + "-bucket";
    if (!bucketName.matches(GS_BUCKET_PATTERN)) {
      throw new GoogleResourceNamingException(
          "Google bucket name '"
              + bucketName
              + "' does not match required pattern for google buckets.");
    }
    return bucketName;
  }

  /**
   * Note: the billing profile used here must be authorized via the profile service before
   * attempting to use it here
   *
   * @param googleProjectId google's id of the project
   * @param billingProfile previously authorized billing profile
   * @param roleIdentityMapping permissions to set
   * @param region region of dataset/snapshot
   * @param labels labels to add to the project
   * @return project resource object
   * @throws InterruptedException if shutting down
   */
  public GoogleProjectResource initializeGoogleProject(
      String googleProjectId,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      GoogleRegion region,
      Map<String, String> labels)
      throws InterruptedException {

    try {
      // If we already have a DR record for this project, return the project resource
      // Should only happen if this step is retried or files are ingested in an existing dataset
      // project
      GoogleProjectResource projectResource =
          resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);
      UUID resourceProfileId = projectResource.getProfileId();
      if (resourceProfileId.equals(billingProfile.getId())) {
        return projectResource;
      }
      throw new MismatchedBillingProfilesException(
          "Cannot reuse existing project "
              + projectResource.getGoogleProjectId()
              + " from profile "
              + resourceProfileId
              + " with a different profile "
              + billingProfile.getId());
    } catch (GoogleResourceNotFoundException e) {
      logger.info("no project resource found for projectId: {}", googleProjectId);
    }

    // Otherwise this project needs to be initialized
    Project project = resourceManagerService.getProject(googleProjectId);
    if (project == null) {
      throw new GoogleResourceException("Could not get project after handout");
    }
    return initializeProject(project, billingProfile, roleIdentityMapping, region, labels);
  }

  public GoogleProjectResource getProjectResourceById(UUID id) {
    return resourceDao.retrieveProjectById(id);
  }

  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    return resourceDao.markUnusedProjectsForDelete(profileId);
  }

  public List<UUID> markUnusedProjectsForDelete(List<UUID> projectResourceIds) {
    return resourceDao.markUnusedProjectsForDelete(projectResourceIds);
  }

  public void deleteUnusedProjects(List<UUID> projectIdList) {
    for (UUID projectId : projectIdList) {
      deleteGoogleProject(projectId);
    }
  }

  public void deleteProjectMetadata(List<UUID> projectIdList) {
    resourceDao.deleteProjectMetadata(projectIdList);
  }

  // Common project initialization for new projects, in the case where we are reusing
  // projects and are missing the metadata for them.
  private GoogleProjectResource initializeProject(
      Project project,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      GoogleRegion region,
      Map<String, String> labels)
      throws InterruptedException {

    String googleProjectNumber = project.getProjectNumber().toString();
    String googleProjectId = project.getProjectId();
    logger.info("google project id: " + googleProjectId);

    GoogleProjectResource googleProjectResource =
        new GoogleProjectResource()
            .profileId(billingProfile.getId())
            .googleProjectId(googleProjectId)
            .googleProjectNumber(googleProjectNumber);

    // The billing profile has already been authorized so we do no further checking here
    billingService.assignProjectBilling(billingProfile, googleProjectResource);

    enableServices(googleProjectResource, region);
    resourceManagerService.updateIamPermissions(
        roleIdentityMapping, googleProjectId, PermissionOp.ENABLE_PERMISSIONS);
    resourceManagerService.addLabelsToProject(googleProjectResource.getGoogleProjectId(), labels);

    UUID id = resourceDao.createProject(googleProjectResource);
    googleProjectResource.id(id);
    return googleProjectResource;
  }

  @VisibleForTesting
  void deleteGoogleProject(UUID resourceId) {
    GoogleProjectResource projectResource = resourceDao.retrieveProjectByIdForDelete(resourceId);
    resourceManagerService.deleteProject(projectResource.getGoogleProjectId());
  }

  @VisibleForTesting
  void enableServices(GoogleProjectResource projectResource, GoogleRegion region)
      throws InterruptedException {
    try {
      ServiceUsage serviceUsage = serviceUsage();
      String projectNumberString = "projects/" + projectResource.getGoogleProjectNumber();
      logger.info(
          "trying to get services for {} ({})",
          projectNumberString,
          projectResource.getGoogleProjectId());
      ServiceUsage.Services.List list =
          serviceUsage.services().list(projectNumberString).setFilter(ENABLED_FILTER);
      ListServicesResponse listServicesResponse = list.execute();

      List<String> requiredServices =
          DATA_PROJECT_SERVICE_IDS.stream()
              .map(s -> String.format("%s/services/%s", projectNumberString, s))
              .collect(Collectors.toList());
      List<GoogleApiServiceusageV1Service> serviceList = listServicesResponse.getServices();
      List<String> actualServiceNames = Collections.emptyList();
      if (serviceList != null) {
        actualServiceNames =
            serviceList.stream()
                .map(GoogleApiServiceusageV1Service::getName)
                .collect(Collectors.toList());
      }
      long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();

      if (actualServiceNames.containsAll(requiredServices)) {
        logger.info("project already has the right resources enabled, skipping");
      } else {
        logger.info("project does not have all resources enabled.");
        BatchEnableServicesRequest batchRequest =
            new BatchEnableServicesRequest().setServiceIds(DATA_PROJECT_SERVICE_IDS);
        ServiceUsage.Services.BatchEnable batchEnable =
            serviceUsage.services().batchEnable(projectNumberString, batchRequest);
        blockUntilServiceOperationComplete(serviceUsage, batchEnable.execute(), timeout);
      }

      enableFirestore(appengine(), projectResource.getGoogleProjectId(), region, timeout);

    } catch (IOException | GeneralSecurityException e) {
      throw new GoogleResourceException("Could not enable services", e);
    }
  }

  public String createProjectServiceAccount(
      String googleProjectId, CollectionType collectionType, String collectionName) {
    try {
      logger.info(
          "Creating service account {} for {} {}",
          SERVICE_ACCOUNT_NAME,
          collectionType.toString().toLowerCase(),
          collectionName);

      GoogleProjectResource projectResource =
          resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);

      ServiceAccount serviceAccount = new ServiceAccount();
      // Note: displayName cannot be longer than 100 characters
      String displayName =
          String.format("SA for %s %s", collectionType.toString().toLowerCase(), collectionName);
      serviceAccount.setDisplayName(displayName.substring(0, Math.min(displayName.length(), 100)));
      CreateServiceAccountRequest request = new CreateServiceAccountRequest();
      request.setAccountId(SERVICE_ACCOUNT_NAME);
      request.setServiceAccount(serviceAccount);

      Iam iamService = iamService();
      serviceAccount =
          iamService
              .projects()
              .serviceAccounts()
              .create("projects/" + projectResource.getGoogleProjectId(), request)
              .execute();

      logger.info("Created service account: {}", serviceAccount.getEmail());

      String datasetSa = String.format("serviceAccount:%s", serviceAccount.getEmail());
      resourceManagerService.updateIamPermissions(
          Map.of(
              "roles/iam.serviceAccountTokenCreator", List.of(datasetSa),
              "roles/serviceusage.serviceUsageConsumer", List.of(datasetSa),
              "roles/storage.objectCreator", List.of(datasetSa),
              "roles/storage.objectViewer", List.of(datasetSa)),
          projectResource.getGoogleProjectId(),
          PermissionOp.ENABLE_PERMISSIONS);

      resourceDao.updateProjectResourceServiceAccount(
          projectResource.getId(), serviceAccount.getEmail());
      logger.info("Set permissions on service accounts");
      return serviceAccount.getEmail();
    } catch (IOException | GeneralSecurityException | InterruptedException e) {
      throw new GoogleResourceException("Unable to create service account", e);
    }
  }

  public enum PermissionOp {
    ENABLE_PERMISSIONS,
    REVOKE_PERMISSIONS
  }

  /** Create a client to speak to the appengine admin api */
  private Appengine appengine() throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new Appengine.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.getApplicationName())
        .build();
  }

  /**
   * Enable Firestore in native mode in an existing project
   *
   * @param appengine appengine client
   * @param googleProjectId name of the google project to create the Firestore DB in
   * @param region the region of the project
   * @param timeout how long to wait for the creation operation
   */
  private static void enableFirestore(
      final Appengine appengine,
      final String googleProjectId,
      final GoogleRegion region,
      final long timeout)
      throws IOException, InterruptedException {
    GoogleRegion firestoreRegion = region.getRegionOrFallbackFirestoreRegion();
    logger.info(
        "Enabling Firestore in project {} in location {}",
        googleProjectId,
        firestoreRegion.toString());
    // Create a request object
    Appengine.Apps.Create createRequest =
        appengine
            .apps()
            .create(
                new Application()
                    .setId(googleProjectId)
                    .setLocationId(firestoreRegion.toString())
                    .setDatabaseType(FIRESTORE_DB_TYPE));

    // Make sure that Firestore is created in the correct project
    createRequest.getRequestHeaders().set("X-Goog-User-Project", googleProjectId);

    // Execute the request
    com.google.api.services.appengine.v1.model.Operation operation = createRequest.execute();

    // Wait for the Firestore creation to finish
    blockUntilAppengineOperationComplete(appengine, operation, googleProjectId, timeout);
    logger.info("Firestore was enabled successfully");
  }

  /**
   * Poll the app engine api until an operation completes. It is possible to hit quota issues here,
   * so the timeout is set to 10 seconds.
   *
   * @param appengine service instance
   * @param operation has an id for us to use in the check
   * @param appId id of the app that the operation is related to
   * @param timeoutSeconds how many seconds before we give up
   * @return a completed operation
   */
  private static com.google.api.services.appengine.v1.model.Operation
      blockUntilAppengineOperationComplete(
          Appengine appengine,
          com.google.api.services.appengine.v1.model.Operation operation,
          String appId,
          long timeoutSeconds)
          throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    final long pollInterval = TimeUnit.SECONDS.toMillis(10);
    final String opName = operation.getName();
    final String opId = extractOperationIdFromName(appId, opName);

    while (operation != null && (operation.getDone() == null || !operation.getDone())) {
      com.google.api.services.appengine.v1.model.Status error = operation.getError();
      if (error != null) {
        throw new AppengineException(
            "Error while waiting for operation to complete" + error.getMessage());
      }
      Thread.sleep(pollInterval);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
        throw new AppengineException("Timed out waiting for operation to complete");
      }
      logger.info("checking operation: {}", opId);
      Appengine.Apps.Operations.Get request = appengine.apps().operations().get(appId, opId);
      // Make sure that the proper project context is set
      request.getRequestHeaders().set("X-Goog-User-Project", appId);
      operation = request.execute();
    }
    return operation;
  }

  @VisibleForTesting
  static String extractOperationIdFromName(final String appId, final String opName) {
    // The format returns is apps/{appId}/operations/{useful id} so we need to extract it
    // Add a check in case they ever change the format
    final String uuidRegex =
        "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";
    final Pattern pattern =
        Pattern.compile(String.format("^apps/%s/operations/(%s)", appId, uuidRegex));
    final Matcher matcher = pattern.matcher(opName);
    if (!matcher.find()) {
      throw new AppengineException(
          String.format("Operation Name does not look as expected: %s", opName));
    }
    return matcher.group(1);
  }

  /**
   * Poll the resource manager api until an operation completes. It is possible to hit quota issues
   * here, so the timeout is set to 10 seconds.
   *
   * @param resourceManager service instance
   * @param operation has an id for us to use in the check
   * @param timeoutSeconds how many seconds before we give up
   * @return a completed operation
   */
  private static Operation blockUntilResourceOperationComplete(
      CloudResourceManager resourceManager, Operation operation, long timeoutSeconds)
      throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    final long pollInterval = TimeUnit.SECONDS.toMillis(10);
    String opId = operation.getName();

    while (operation != null && (operation.getDone() == null || !operation.getDone())) {
      Status error = operation.getError();
      if (error != null) {
        throw new GoogleResourceException(
            "Error while waiting for operation to complete" + error.getMessage());
      }
      Thread.sleep(pollInterval);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
        throw new GoogleResourceException("Timed out waiting for operation to complete");
      }
      logger.info("checking operation: {}", opId);
      CloudResourceManager.Operations.Get request = resourceManager.operations().get(opId);
      operation = request.execute();
    }
    return operation;
  }

  private ServiceUsage serviceUsage() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new ServiceUsage.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.getApplicationName())
        .build();
  }

  // Initialize the IAM service, which can be used to send requests to the IAM API.
  private Iam iamService() throws GeneralSecurityException, IOException {
    GoogleCredentials credential =
        GoogleCredentials.getApplicationDefault()
            .createScoped(Collections.singleton(IamScopes.CLOUD_PLATFORM));
    Iam service =
        new Iam.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(credential))
            .setApplicationName(ApplicationConfiguration.APPLICATION_NAME)
            .build();
    return service;
  }

  private static com.google.api.services.serviceusage.v1.model.Operation
      blockUntilServiceOperationComplete(
          ServiceUsage serviceUsage,
          com.google.api.services.serviceusage.v1.model.Operation operation,
          long timeoutSeconds)
          throws IOException, InterruptedException {

    long start = System.currentTimeMillis();
    final long pollInterval = TimeUnit.SECONDS.toMillis(5);
    String opId = operation.getName();

    while (operation != null && (operation.getDone() == null || !operation.getDone())) {
      com.google.api.services.serviceusage.v1.model.Status error = operation.getError();
      if (error != null) {
        throw new GoogleResourceException(
            "Error while waiting for operation to complete" + error.getMessage());
      }
      Thread.sleep(pollInterval);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
        throw new GoogleResourceException("Timed out waiting for operation to complete");
      }
      ServiceUsage.Operations.Get request = serviceUsage.operations().get(opId);
      operation = request.execute();
    }
    return operation;
  }

  public void updateProjectsBillingAccount(BillingProfileModel billingProfileModel) {
    List<GoogleProjectResource> projects =
        resourceDao.retrieveProjectsByBillingProfileId(billingProfileModel.getId());

    if (projects.size() == 0) {
      logger.info("No projects attached to billing profile so nothing to update.");
      return;
    }

    projects.stream()
        .forEach(
            project -> {
              logger.info(
                  "Updating billing profile id {} in google project {}",
                  billingProfileModel.getBillingAccountId(),
                  project.getGoogleProjectId());
              updateBillingProfile(
                  project.getGoogleProjectNumber(),
                  project.getGoogleProjectId(),
                  billingProfileModel);
            });
  }

  public void updateBillingProfile(
      String googleProjectNumber, String googleProjectId, BillingProfileModel billingProfile) {
    GoogleProjectResource googleProjectResource =
        new GoogleProjectResource()
            .profileId(billingProfile.getId())
            .googleProjectId(googleProjectId)
            .googleProjectNumber(googleProjectNumber);

    // The billing profile has already been authorized so we do no further checking here
    billingService.assignProjectBilling(billingProfile, googleProjectResource);
  }

  @VisibleForTesting
  static void ensureValidProjectId(final String projectId) {
    Preconditions.checkNotNull(projectId, "Project Id must not be null");

    Preconditions.checkArgument(
        projectId.matches("^[a-z][a-z0-9-]{4,28}[a-z0-9]$"),
        String.format(
            "The project ID \"%s\" must be a unique string of 6 to 30 lowercase letters, digits, "
                + "or hyphens. It must start with a letter, and cannot have a trailing hyphen. You cannot change a "
                + "project ID once it has been created. You cannot re-use a project ID that is in use, or one that "
                + "has been used for a deleted project.",
            projectId));
  }
}
