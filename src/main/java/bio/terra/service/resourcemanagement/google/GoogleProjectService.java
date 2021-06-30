package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.exception.AppengineException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.MismatchedBillingProfilesException;
import bio.terra.service.resourcemanagement.exception.UpdatePermissionsFailedException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.api.services.cloudresourcemanager.model.SetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.serviceusage.v1.ServiceUsage;
import com.google.api.services.serviceusage.v1.model.BatchEnableServicesRequest;
import com.google.api.services.serviceusage.v1.model.GoogleApiServiceusageV1Service;
import com.google.api.services.serviceusage.v1.model.ListServicesResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
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

  private final GoogleBillingService billingService;
  private final GoogleResourceDao resourceDao;
  private final GoogleResourceConfiguration resourceConfiguration;
  private final ObjectMapper objectMapper;
  private final ConfigurationService configService;
    private final BufferService bufferService;

  @Autowired
  public GoogleProjectService(
      GoogleResourceDao resourceDao,
      GoogleResourceConfiguration resourceConfiguration,
      GoogleBillingService billingService,
      ConfigurationService configService,
      ObjectMapper objectMapper,
    BufferService bufferService) {
        this.resourceDao = resourceDao;
        this.resourceConfiguration = resourceConfiguration;
        this.billingService = billingService;
        this.configService = configService;
        this.objectMapper = objectMapper;
        this.bufferService = bufferService;
    }

  /**
   * Note: the billing profile used here must be authorized via the profile service before
   * attempting to use it here
   *
   * @param googleProjectId google's id of the project
   * @param billingProfile previously authorized billing profile
   * @param roleIdentityMapping permissions to set
   * @param region region of dataset/snapshot
   * @return project resource object
   * @throws InterruptedException if shutting down
   */
  public GoogleProjectResource getOrCreateProject(
      String googleProjectId,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      GoogleRegion region)
      throws InterruptedException {

    try {
      GoogleProjectResource projectResource =
          resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);
      UUID resourceProfileId = projectResource.getProfileId();
      if (resourceProfileId.equals(billingProfile.getId())) {
                return projectResource;
            }
            throw new MismatchedBillingProfilesException(
                "Cannot reuse existing project " + projectResource.getGoogleProjectId()
              + " from profile "
              + resourceProfileId
              + " with a different profile "
              + billingProfile.getId());
    } catch (GoogleResourceNotFoundException e) {
      logger.info("no project resource found for projectId: {}", googleProjectId);
    }

    // In development we are willing to reuse projects that exist already, but are not stored
    // in the database. In that case, we reinitialize them, but do not change their associated
    // billing account.
    // In production, it is hazardous to use projects that exist since we do not know where they
    // came from, what resources they contain, who is paying for them.
    Project existingProject = getProject(googleProjectId);
    if (existingProject != null && resourceConfiguration.getAllowReuseExistingProjects()) {
      return initializeProject(existingProject, billingProfile, roleIdentityMapping, false, region);
    }

    return newProject(googleProjectId, billingProfile, roleIdentityMapping, region);
  }

  /**
     * Note: the billing profile used here must be authorized via the
     * profile service before attempting to use it here
     *
     * @param googleProjectId     google's id of the project
     * @param billingProfile      previously authorized billing profile
     * @param roleIdentityMapping permissions to set
     * @param region              region of dataset/snapshot
     * @return project resource object
     * @throws InterruptedException if shutting down
     */
    public GoogleProjectResource getOrInitializeProject(
            String googleProjectId,
            BillingProfileModel billingProfile,
            Map<String, List<String>> roleIdentityMapping,
            GoogleRegion region)
            throws InterruptedException {

        try {
            // If we already have a DR record for this project, return the project resource
            // Should only happen if this step is retried or files are ingested in an existing dataset project
            GoogleProjectResource projectResource = resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);
            UUID resourceProfileId = projectResource.getProfileId();
            if (resourceProfileId.equals(billingProfile.getId())) {
                return projectResource;
            }
            throw new MismatchedBillingProfilesException(
                    "Cannot reuse existing project " + projectResource.getGoogleProjectId() +
                            " from profile " + resourceProfileId +
                            " with a different profile " + billingProfile.getId());
        } catch (GoogleResourceNotFoundException e) {
            logger.info("no project resource found for projectId: {}", googleProjectId);
        }

        // Otherwise this project needs to be initialized
        Project project = getProject(googleProjectId);
        if (project == null) {
            throw new GoogleResourceException("Could not get project after handout");
        }
        return initializeProject(project, billingProfile, roleIdentityMapping, false, region);

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

  /**
   * Created a new google project. This process is not transactional or done in a stairway flight,
   * so it is possible we will allocate projects and before they are recorded in our database, we
   * will fail and they will be orphaned. We expect that the Resource Buffing Service will be
   * performing project creation for us eventually so we will not do the work two fix those failure
   * windows.
   *
   * @param requestedProjectId suggested name for the project
   * @param billingProfile authorized billing profile that'll pay for the project
   * @param roleIdentityMapping iam roles to be granted on the project
   * @param region region of the dataset/snapshot
   * @return a populated project resource object
   * @throws InterruptedException if the flight is interrupted during execution
   */
  private GoogleProjectResource newProject(
      String requestedProjectId,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      GoogleRegion region)
      throws InterruptedException {

    ensureValidProjectId(requestedProjectId);

    // projects created by service accounts must live under a parent resource (either a folder or an
    // organization)
    ResourceId parentResource =
        new ResourceId()
            .setType(resourceConfiguration.getParentResourceType())
            .setId(resourceConfiguration.getParentResourceId());
    Project requestBody =
        new Project()
            .setName(requestedProjectId)
            .setProjectId(requestedProjectId)
            .setParent(parentResource);
    logger.info("creating project with request: {}", requestBody);
    try {
      // kick off a project create request and poll until it is done
      CloudResourceManager resourceManager = cloudResourceManager();
      CloudResourceManager.Projects.Create request = resourceManager.projects().create(requestBody);
      Operation operation = request.execute();
      long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
      blockUntilResourceOperationComplete(resourceManager, operation, timeout);

      // TODO: What happens if the requested project id is not the actual project id?
      // it should be retrievable once the create operation is complete
      Project project = getProject(requestedProjectId);
      if (project == null) {
        throw new GoogleResourceException("Could not get project after creation");
      }

      return initializeProject(project, billingProfile, roleIdentityMapping, true, region);
    } catch (IOException | GeneralSecurityException e) {
      throw new GoogleResourceException("Could not create project", e);
    }
  }

  // Common project initialization for new projects, in the case where we are reusing
  // projects and are missing the metadata for them.
  private GoogleProjectResource initializeProject(
      Project project,
      BillingProfileModel billingProfile,
      Map<String, List<String>> roleIdentityMapping,
      boolean setBilling,
      GoogleRegion region)
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
      // In development we are reusing projects. The TDR service account may not have permission to
      // properly enable the services on a developer's project. In those cases, we do not want to
      // error on failure. That result in issues down the line if we require enabling new services.
      if (!resourceConfiguration.getAllowReuseExistingProjects()) {
        throw new GoogleResourceException("Could not enable services", e);
      }
    }
  }

  private static final int RETRIES = 10;
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int INITIAL_WAIT_SECONDS = 2;

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

    Exception lastException = null;
    int retryWait = INITIAL_WAIT_SECONDS;
    for (int i = 0; i < RETRIES; i++) {
      try {
        CloudResourceManager resourceManager = cloudResourceManager();
        Policy policy =
            resourceManager.projects().getIamPolicy(projectId, getIamPolicyRequest).execute();
        final List<Binding> bindingsList = policy.getBindings();

        switch (permissionOp) {
          case ENABLE_PERMISSIONS:
            for (Map.Entry<String, List<String>> entry : userPermissions.entrySet()) {
              Binding binding = new Binding().setRole(entry.getKey()).setMembers(entry.getValue());
              bindingsList.add(binding);
            }
            break;

          case REVOKE_PERMISSIONS:
            // Remove members from the current policies
            for (Map.Entry<String, List<String>> entry : userPermissions.entrySet()) {
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
        return;
      } catch (IOException | GeneralSecurityException ex) {
        logger.info("Failed to enable iam permissions. Retry " + i + " of " + RETRIES, ex);
        lastException = ex;
      }

      TimeUnit.SECONDS.sleep(retryWait);
      retryWait = retryWait + retryWait;
      if (retryWait > MAX_WAIT_SECONDS) {
        retryWait = MAX_WAIT_SECONDS;
      }
    }
    throw new UpdatePermissionsFailedException("Cannot update iam permissions", lastException);
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
