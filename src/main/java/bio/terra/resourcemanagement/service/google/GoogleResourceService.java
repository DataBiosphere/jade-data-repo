package bio.terra.resourcemanagement.service.google;

import bio.terra.resourcemanagement.dao.google.GoogleResourceNotFoundException;
import bio.terra.resourcemanagement.dao.google.GoogleResourceDao;
import bio.terra.flight.exception.InaccessibleBillingAccountException;
import bio.terra.metadata.BillingProfile;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectRequest;
import bio.terra.resourcemanagement.service.ProfileService;
import bio.terra.resourcemanagement.service.exception.GoogleResourceException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.Operation;
import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.api.services.serviceusage.v1beta1.model.BatchEnableServicesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.UUID;

@Component
public class GoogleResourceService {
    private Logger logger = LoggerFactory.getLogger(GoogleResourceService.class);

    private final GoogleResourceDao resourceDao;
    private final ProfileService profileService;
    private final GoogleResourceConfiguration resourceConfiguration;
    private final GoogleBillingService billingService;

    @Autowired
    public GoogleResourceService(
            GoogleResourceDao resourceDao,
            ProfileService profileService,
            GoogleResourceConfiguration resourceConfiguration,
            GoogleBillingService billingService) {
        this.resourceDao = resourceDao;
        this.profileService = profileService;
        this.resourceConfiguration = resourceConfiguration;
        this.billingService = billingService;
    }

    public GoogleProjectResource getOrCreateProject(GoogleProjectRequest projectRequest) {
        // Naive: this implements a 1-project-per-profile approach. If there is already a Google project for this
        // profile we will look up the project by id, otherwise we will generate one and look it up
        UUID profileId = projectRequest.getProfileId();
        try {
            return resourceDao.retrieveProjectById(profileId);

        } catch (GoogleResourceNotFoundException e) {
            logger.info(String.format("no metadata found for profile: %s", profileId));
        }

        // it's possible that the project exists already but it is not stored in the metadata table
        String googleProjectId = projectRequest.getProjectId();
        Project existingProject = getProject(googleProjectId);
        if (existingProject != null) {
            GoogleProjectResource googleProjectResource = new GoogleProjectResource(projectRequest)
                .googleProjectId(googleProjectId)
                .googleProjectNumber(existingProject.getProjectNumber().toString());
            enableServices(googleProjectResource);
            UUID id = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(id);
        }

        return newProject(projectRequest, googleProjectId);
    }

    public GoogleProjectResource getProjectResourceById(UUID id) {
        return resourceDao.retrieveProjectById(id);
    }

    private Project getProject(String googleProjectId) {
        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Get request = resourceManager.projects().get(googleProjectId);
            return request.execute();
        } catch (GoogleJsonResponseException e) {
            // if the project does not exist, the API will return a 403 unauth. to prevent people probing for projects
            if (e.getDetails().getCode() != 403) {
                throw new GoogleResourceException("Unexpected error while checking on project state", e);
            }
            return null;
        } catch (IOException | GeneralSecurityException e) {
            throw new GoogleResourceException("Could not check on project state", e);
        }
    }

    private GoogleProjectResource newProject(GoogleProjectRequest projectRequest, String googleProjectId) {
        BillingProfile profile = profileService.getProfileById(projectRequest.getProfileId());
        if (!profile.isAccessible()) {
            throw new InaccessibleBillingAccountException("The repository needs access to this billing account " +
                "in order to create: " + googleProjectId);
        }

        Project requestBody = new Project()
            .setName(googleProjectId)
            .setProjectId(googleProjectId);
        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Create request = resourceManager.projects().create(requestBody);
            Operation operation = request.execute();
            long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
            blockUntilResourceOperationComplete(resourceManager, operation, timeout);
            Project project = getProject(googleProjectId);
            if (project == null) {
                throw new GoogleResourceException("Could not get project after creation");
            }
            String googleProjectNumber = project.getProjectNumber().toString();
            GoogleProjectResource googleProjectResource = new GoogleProjectResource(projectRequest)
                .googleProjectId(googleProjectId)
                .googleProjectNumber(googleProjectNumber);
            setupBilling(googleProjectResource);
            enableServices(googleProjectResource);
            UUID repositoryId = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(repositoryId);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not create project", e);
        }
    }

    private void enableServices(GoogleProjectResource projectResource) {
        BatchEnableServicesRequest batchRequest = new BatchEnableServicesRequest()
            .setServiceIds(projectResource.getServiceIds());
        try {
            ServiceUsage serviceUsage = serviceUsage();
            ServiceUsage.Services.BatchEnable batchEnable = serviceUsage.services()
                .batchEnable("projects/" + projectResource.getGoogleProjectNumber(), batchRequest);
            long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
            blockUntilServiceOperationComplete(serviceUsage, batchEnable.execute(), timeout);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not enable services", e);
        }
    }

    private void setupBilling(GoogleProjectResource project) {
        BillingProfile billingProfile = profileService.getProfileById(project.getProfileId());
        billingService.assignProjectBilling(billingProfile, project);
    }

    private CloudResourceManager cloudResourceManager() throws IOException, GeneralSecurityException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        if (credential.createScopedRequired()) {
            credential =
                credential.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
        }

        return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(resourceConfiguration.getApplicationName())
            .build();
    }

    private static Operation blockUntilResourceOperationComplete(
            CloudResourceManager resourceManager,
            Operation operation,
            long timeoutSeconds) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000; // 5 seconds
        String opId = operation.getName();

        while (operation != null && (operation.getDone() == null || !operation.getDone())) {
            Status error = operation.getError();
            if (error != null) {
                throw new GoogleResourceException("Error while waiting for operation to complete" + error.getMessage());
            }
            Thread.sleep(pollInterval);
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed >= timeoutSeconds * 1000) {
                throw new GoogleResourceException("Timed out waiting for operation to complete");
            }
            CloudResourceManager.Operations.Get request = resourceManager.operations().get(opId);
            operation = request.execute();
        }
        return operation;
    }

    private ServiceUsage serviceUsage() throws IOException, GeneralSecurityException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        return new ServiceUsage.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(resourceConfiguration.getApplicationName())
            .build();
    }

    private static com.google.api.services.serviceusage.v1beta1.model.Operation blockUntilServiceOperationComplete(
            ServiceUsage serviceUsage,
            com.google.api.services.serviceusage.v1beta1.model.Operation operation,
            long timeoutSeconds) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000; // 5 seconds
        String opId = operation.getName();

        while (operation != null && (operation.getDone() == null || !operation.getDone())) {
            com.google.api.services.serviceusage.v1beta1.model.Status error = operation.getError();
            if (error != null) {
                throw new GoogleResourceException("Error while waiting for operation to complete" + error.getMessage());
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
