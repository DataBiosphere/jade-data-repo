package bio.terra.service.resourcemanagement.google;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.UpdatePermissionsFailedException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
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
import com.google.api.services.serviceusage.v1.model.ListServicesResponse;
import com.google.api.services.serviceusage.v1.model.GoogleApiServiceusageV1Service;
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
import java.util.stream.Collectors;

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
                "cloudbilling.googleapis.com"));

    private final GoogleBillingService billingService;
    private final GoogleResourceDao resourceDao;
    private final GoogleResourceConfiguration resourceConfiguration;
    private final ConfigurationService configService;

    @Autowired
    public GoogleProjectService(
        GoogleResourceDao resourceDao,
        GoogleResourceConfiguration resourceConfiguration,
        GoogleBillingService billingService,
        ConfigurationService configService) {
        this.resourceDao = resourceDao;
        this.resourceConfiguration = resourceConfiguration;
        this.billingService = billingService;
        this.configService = configService;
    }

    public GoogleProjectResource getOrCreateProject(
        String googleProjectId,
        BillingProfileModel billingProfile,
        Map<String, List<String>> roleIdentityMapping)
        throws InterruptedException {

        try {
            return resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);
        } catch (GoogleResourceNotFoundException e) {
            logger.info("no project resource found for projectId: {}", googleProjectId);
        }

        // In development we are willing to reuse projects that exist already, but are not stored
        // in the database. In that case, we reinitialize them, but do not change their associated
        // billing account.
        // In production, it is hazardous to use projects that exist since we do not know where they
        // came from, what resources they contain, who is paying for them.
        // TODO: change the boolean when we have plumbed in the allowReuseExistingProjects flag.
        Project existingProject = getProject(googleProjectId);
        if (existingProject != null) { // TODO: && resourceConfiguration.getAllowReuseExistingProjects()) {
            return initializeProject(existingProject, billingProfile, roleIdentityMapping, false);
        }

        return newProject(googleProjectId, billingProfile, roleIdentityMapping);
    }

    public GoogleProjectResource getProjectResourceById(UUID id) {
        return resourceDao.retrieveProjectById(id);
    }

    // package access for use in tests
    Project getProject(String googleProjectId) {
        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Get request = resourceManager.projects().get(googleProjectId);
            return request.execute();
        } catch (GoogleJsonResponseException e) {
            // if the project does not exist, the API will return a 403 unauth. to prevent people probing
            // for projects
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
     * @param requestedProjectId  suggested name for the project
     * @param billingProfile      authorized billing profile that'll pay for the project
     * @param roleIdentityMapping iam roles to be granted on the project
     * @return a populated project resource object
     * @throws InterruptedException if the flight is interrupted during execution
     */
    private GoogleProjectResource newProject(
        String requestedProjectId,
        BillingProfileModel billingProfile,
        Map<String, List<String>> roleIdentityMapping)
        throws InterruptedException {

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

            return initializeProject(project, billingProfile, roleIdentityMapping, true);
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
        boolean setBilling)
        throws InterruptedException {

        String googleProjectNumber = project.getProjectNumber().toString();
        String googleProjectId = project.getProjectId();

        GoogleProjectResource googleProjectResource =
            new GoogleProjectResource()
                .profileId(UUID.fromString(billingProfile.getId()))
                .googleProjectId(googleProjectId)
                .googleProjectNumber(googleProjectNumber);

        if (setBilling) {
            if (billingService.canAccess(billingProfile)) {
                billingService.assignProjectBilling(billingProfile, googleProjectResource);
            }
        }
        enableServices(googleProjectResource);
        updateIamPermissions(roleIdentityMapping, googleProjectId, PermissionOp.ENABLE_PERMISSIONS);

        UUID id = resourceDao.createProject(googleProjectResource);
        googleProjectResource.id(id);
        return googleProjectResource;
    }

    private void deleteGoogleProject(String projectId) {
        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Delete request = resourceManager.projects().delete(projectId);
            // the response will be empty if the request is successful in the delete
            request.execute();
        } catch (IOException | GeneralSecurityException e) {
            throw new GoogleResourceException("Could not delete project", e);
        }
    }

    // TODO: check dependencies before delete
    // package access for use in tests
    void deleteProjectResource(UUID resourceId) {
        GoogleProjectResource projectResource = resourceDao.retrieveProjectById(resourceId);
        deleteGoogleProject(projectResource.getGoogleProjectId());
        resourceDao.deleteProject(resourceId);
    }

    private void enableServices(GoogleProjectResource projectResource) throws InterruptedException {
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
                    serviceList.stream().map(GoogleApiServiceusageV1Service::getName).collect(Collectors.toList());
            }

            if (actualServiceNames.containsAll(requiredServices)) {
                logger.info("project already has the right resources enabled, skipping");
            } else {
                logger.info("project does not have all resources enabled.");
                BatchEnableServicesRequest batchRequest =
                    new BatchEnableServicesRequest().setServiceIds(DATA_PROJECT_SERVICE_IDS);
                ServiceUsage.Services.BatchEnable batchEnable =
                    serviceUsage.services().batchEnable(projectNumberString, batchRequest);
                long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
                blockUntilServiceOperationComplete(serviceUsage, batchEnable.execute(), timeout);
            }
        } catch (IOException | GeneralSecurityException e) {
            throw new GoogleResourceException("Could not enable services", e);
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

    /**
     * Poll the resource manager api until an operation completes. It is possible to hit quota issues
     * here, so the timeout is set to 10 seconds.
     *
     * @param resourceManager service instance
     * @param operation       has an id for us to use in the check
     * @param timeoutSeconds  how many seconds before we give up
     * @return a completed operation
     */
    private static Operation blockUntilResourceOperationComplete(
        CloudResourceManager resourceManager, Operation operation, long timeoutSeconds)
        throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final long pollInterval = 10 * 1000; // 10 seconds
        String opId = operation.getName();

        while (operation != null && (operation.getDone() == null || !operation.getDone())) {
            Status error = operation.getError();
            if (error != null) {
                throw new GoogleResourceException(
                    "Error while waiting for operation to complete" + error.getMessage());
            }
            Thread.sleep(pollInterval);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= timeoutSeconds * 1000) {
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

    private static com.google.api.services.serviceusage.v1.model.Operation blockUntilServiceOperationComplete(
        ServiceUsage serviceUsage,
        com.google.api.services.serviceusage.v1.model.Operation operation,
        long timeoutSeconds)
        throws IOException, InterruptedException {

        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000; // 5 seconds
        String opId = operation.getName();

        while (operation != null && (operation.getDone() == null || !operation.getDone())) {
            com.google.api.services.serviceusage.v1.model.Status error = operation.getError();
            if (error != null) {
                throw new GoogleResourceException(
                    "Error while waiting for operation to complete" + error.getMessage());
            }
            Thread.sleep(pollInterval);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= timeoutSeconds * 1000) {
                throw new GoogleResourceException("Timed out waiting for operation to complete");
            }
            ServiceUsage.Operations.Get request = serviceUsage.operations().get(opId);
            operation = request.execute();
        }
        return operation;
    }
}
