package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.AclUtils;
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
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.Application;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Binding;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Operation;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.SetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.serviceusage.v1.ServiceUsage;
import com.google.api.services.serviceusage.v1.model.BatchEnableServicesRequest;
import com.google.api.services.serviceusage.v1.model.GoogleApiServiceusageV1Service;
import com.google.api.services.serviceusage.v1.model.ListServicesResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
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

  private final GoogleBillingService billingService;
  private final GoogleResourceDao resourceDao;
  private final GoogleResourceConfiguration resourceConfiguration;
  private final BufferService bufferService;
  private final DatasetBucketDao datasetBucketDao;
  private final Environment environment;

  private static final String GS_BUCKET_PATTERN = "[a-z0-9\\-\\.\\_]{3,63}";

  @Autowired
  public GoogleProjectService(
      GoogleResourceDao resourceDao,
      GoogleResourceConfiguration resourceConfiguration,
      GoogleBillingService billingService,
      BufferService bufferService,
      DatasetBucketDao datasetBucketDao,
      Environment environment) {
    this.resourceDao = resourceDao;
    this.resourceConfiguration = resourceConfiguration;
    this.billingService = billingService;
    this.bufferService = bufferService;
    this.datasetBucketDao = datasetBucketDao;
    this.environment = environment;
  }

  public String bucketForBigQueryScratchFile(String googleProjectId) {
    return googleProjectId + "-bq-scratch-bucket";
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
    ResourceInfo resource = bufferService.handoutResource();
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
    Project project = getProject(googleProjectId);
    if (project == null) {
      throw new GoogleResourceException("Could not get project after handout");
    }
    return initializeProject(project, billingProfile, roleIdentityMapping, false, region, labels);
  }

  public GoogleProjectResource getProjectResourceById(UUID id) {
    return resourceDao.retrieveProjectById(id);
  }

  public List<UUID> markUnusedProjectsForDelete(UUID profileId) {
    return resourceDao.markUnusedProjectsForDelete(profileId);
  }

  public void deleteUnusedProjects(List<UUID> projectIdList) {
    for (UUID projectId : projectIdList) {
      deleteGoogleProject(projectId);
    }
  }

  public void deleteProjectMetadata(List<UUID> projectIdList) {
    resourceDao.deleteProjectMetadata(projectIdList);
  }

  // package access for use in tests
  public Project getProject(String googleProjectId) {
    try {
      CloudResourceManager resourceManager = cloudResourceManager();
      CloudResourceManager.Projects.Get request = resourceManager.projects().get(googleProjectId);
      return request.execute();
    } catch (GoogleJsonResponseException e) {
      // if the project does not exist, the API will return a 403 unauth. to prevent people probing
      // for projects. We tolerate non-existent projects, because we want to be able to retry
      // failures on deleting other projects.
      if (e.getDetails().getCode() != 403) {
        throw new GoogleResourceException("Unexpected error while checking on project state", e);
      }
      return null;
    } catch (IOException | GeneralSecurityException e) {
      throw new GoogleResourceException("Could not check on project state", e);
    }
  }

  // Common project initialization for new projects, in the case where we are reusing
  // projects and are missing the metadata for them.
  private GoogleProjectResource initializeProject(
      Project project,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      boolean setBilling,
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

    if (setBilling) {
      // The billing profile has already been authorized so we do no further checking here
      billingService.assignProjectBilling(billingProfile, googleProjectResource);
    }
    enableServices(googleProjectResource, region);
    updateIamPermissions(roleIdentityMapping, googleProjectId, PermissionOp.ENABLE_PERMISSIONS);
    addLabelsToProject(googleProjectResource.getGoogleProjectId(), labels);

    UUID id = resourceDao.createProject(googleProjectResource);
    googleProjectResource.id(id);
    return googleProjectResource;
  }

  private void deleteGoogleProject(String googleProjectId) {
    // Don't actually delete the project if we are reusing projects!
    if (resourceConfiguration.getAllowReuseExistingProjects()) {
      logger.info("Reusing projects: skipping delete of {}", googleProjectId);
    } else {
      try {
        CloudResourceManager resourceManager = cloudResourceManager();
        CloudResourceManager.Projects.Delete request =
            resourceManager.projects().delete(googleProjectId);
        // the response will be empty if the request is successful in the delete
        request.execute();
      } catch (IOException | GeneralSecurityException e) {
        throw new GoogleResourceException("Could not delete project", e);
      }
    }
  }

  @VisibleForTesting
  void deleteGoogleProject(UUID resourceId) {
    GoogleProjectResource projectResource = resourceDao.retrieveProjectByIdForDelete(resourceId);
    deleteGoogleProject(projectResource.getGoogleProjectId());
  }

  /**
   * Google requires labels to consist of only lowercase, alphanumeric, "_", and "-" characters.
   * This value can be at most 63 characters long.
   *
   * @param string String to clean
   * @return The cleaned String
   */
  @VisibleForTesting
  static String cleanForLabels(String string) {
    return string
        .toLowerCase(Locale.ROOT)
        .trim()
        .replaceAll("[^a-z0-9_-]", "-")
        .substring(0, Math.min(string.length(), 63));
  }

  public void addLabelsToProject(String googleProjectId, Map<String, String> labels) {
    final Stream<Map.Entry<String, String>> additionalLabels;
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(env -> env.contains("test") || env.contains("int"))) {
      additionalLabels = Stream.of(Map.entry("project-for-test", "true"));
    } else {
      additionalLabels = Stream.empty();
    }

    try {
      CloudResourceManager resourceManager = cloudResourceManager();
      Project project = resourceManager.projects().get(googleProjectId).execute();
      Map<String, String> cleanedLabels =
          Stream.concat(
                  Stream.concat(project.getLabels().entrySet().stream(), additionalLabels),
                  labels.entrySet().stream()
                      .map(
                          e -> Map.entry(cleanForLabels(e.getKey()), cleanForLabels(e.getValue()))))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1));
      project.setLabels(cleanedLabels);
      logger.info("Adding labels to project {}", googleProjectId);
      resourceManager.projects().update(googleProjectId, project).execute();
    } catch (Exception ex) {
      // only a soft failure - we do not want to fail project create just on adding project labels
      logger.warn("Encountered error while updating project labels. ex: {}, stacktrace: {}", ex);
    }
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

  public enum PermissionOp {
    ENABLE_PERMISSIONS,
    REVOKE_PERMISSIONS
  }

  // Set permissions on a project
  public void updateIamPermissions(
      Map<String, List<String>> userPermissions, String projectId, PermissionOp permissionOp)
      throws InterruptedException {

    // Nothing to do if no permissions updates are requested
    if (userPermissions == null || userPermissions.size() == 0) {
      return;
    }

    GetIamPolicyRequest getIamPolicyRequest = new GetIamPolicyRequest();

    AclUtils.aclUpdateRetry(
        () -> {
          try {
            CloudResourceManager resourceManager = cloudResourceManager();
            Policy policy =
                resourceManager.projects().getIamPolicy(projectId, getIamPolicyRequest).execute();
            final List<Binding> bindingsList = policy.getBindings();

            switch (permissionOp) {
              case ENABLE_PERMISSIONS:
                for (var entry : userPermissions.entrySet()) {
                  Binding binding =
                      new Binding().setRole(entry.getKey()).setMembers(entry.getValue());
                  bindingsList.add(binding);
                }
                break;

              case REVOKE_PERMISSIONS:
                // Remove members from the current policies
                for (var entry : userPermissions.entrySet()) {
                  CollectionUtils.filter(
                      bindingsList,
                      b -> {
                        if (Objects.equals(b.getRole(), entry.getKey())) {
                          // Remove the members that were passed in
                          b.setMembers(ListUtils.subtract(b.getMembers(), entry.getValue()));
                          // Remove any entries from the bindings list with no members
                          return !b.getMembers().isEmpty();
                        }
                        return true;
                      });
                }
            }

            policy.setBindings(bindingsList);
            SetIamPolicyRequest setIamPolicyRequest = new SetIamPolicyRequest().setPolicy(policy);
            resourceManager.projects().setIamPolicy(projectId, setIamPolicyRequest).execute();
            return null;
          } catch (IOException | GeneralSecurityException ex) {
            throw new AclUtils.AclRetryException(
                "Encountered an error while updating IAM permissions", ex, ex.getMessage());
          }
        });
  }

  // TODO: convert this to using the resource manager service interface instead of the api interface
  //  https://googleapis.dev/java/google-cloud-resourcemanager/latest/index.html
  //     ?com/google/cloud/resourcemanager/ResourceManager.html
  //  And use GoogleCredentials instead of the deprecated class. (DR-1459)
  private CloudResourceManager cloudResourceManager() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.getApplicationName())
        .build();
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
