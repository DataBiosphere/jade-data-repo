package bio.terra.service.google;

import bio.terra.dao.exception.google.ProjectNotFoundException;
import bio.terra.dao.google.GoogleResourceDao;
import bio.terra.flight.exception.InaccessibleBillingAccountException;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Study;
import bio.terra.metadata.google.DataProject;
import bio.terra.metadata.google.DataProjectRequest;
import bio.terra.service.ProfileService;
import bio.terra.service.exception.GoogleResourceException;
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
import java.util.List;
import java.util.UUID;

@Component
public class GoogleResourceService {
    private Logger logger = LoggerFactory.getLogger(GoogleResourceService.class);

    private final GoogleResourceDao resourceDao;
    private final ProfileService profileService;
    private final GoogleResourceConfiguration resourceConfiguration;
    private final GoogleBillingService billingService;
    private final GoogleProjectIdSelector googleProjectIdSelector;

    @Autowired
    public GoogleResourceService(
            GoogleResourceDao resourceDao,
            ProfileService profileService,
            GoogleResourceConfiguration resourceConfiguration,
            GoogleBillingService billingService,
            GoogleProjectIdSelector googleProjectIdSelector) {
        this.resourceDao = resourceDao;
        this.profileService = profileService;
        this.resourceConfiguration = resourceConfiguration;
        this.billingService = billingService;
        this.googleProjectIdSelector = googleProjectIdSelector;
    }

    public DataProject getProjectForDataset(Dataset dataset) {
        DataProjectRequest project = new DataProjectRequest()
            .datasetId(dataset.getId())
            .profileId(dataset.getProfile().getId());
        return getOrCreateProject(project);
    }

    public DataProject getProjectForStudy(Study study) {
        DataProjectRequest project = new DataProjectRequest()
            .studyId(study.getId())
            .profileId(study.getDefaultProfileId());
        DataProject dataProject = getOrCreateProject(project);
        return dataProject;
    }

    public DataProject getOrCreateProject(DataProjectRequest projectRequest) {
        // Naive: this implements a 1-project-per-profile approach. If there is already a Google project for this
        // profile we will look up the project by id, otherwise we will generate one and look it up
        UUID profileId = projectRequest.getProfileId();
        try {
            return resourceDao.retrieveProjectBy("profile_id", profileId);

        } catch (ProjectNotFoundException e) {
            logger.info(String.format("no metadata found for profile: %s", profileId));
        }
        String googleProjectId = googleProjectIdSelector.projectId(projectRequest);

        // it's possible that the project exists already but it is not stored in the metadata table
        Project existingProject = getProject(googleProjectId);
        if (existingProject != null) {
            String googleProjectNumber = existingProject.getProjectNumber().toString();
            enableServices(googleProjectNumber);
            DataProject dataProject = new DataProject(projectRequest)
                .googleProjectId(googleProjectId)
                .googleProjectNumber(googleProjectNumber);
            UUID id = resourceDao.createProject(dataProject);
            return dataProject.repositoryId(id);
        }

        return newProject(projectRequest, googleProjectId);
    }

    public Project getProject(String googleProjectId) {
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

    public DataProject newProject(DataProjectRequest projectRequest, String googleProjectId) {
        BillingProfile profile = profileService.getProfileById(projectRequest.getProfileId());
        if (!profile.isAccessible()) {
            throw new InaccessibleBillingAccountException("The repository needs access to this billing account");
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
            DataProject dataProject = new DataProject(projectRequest)
                .googleProjectId(googleProjectId)
                .googleProjectNumber(googleProjectNumber);
            setupBilling(dataProject);
            enableServices(googleProjectNumber);
            UUID repositoryId = resourceDao.createProject(dataProject);
            return dataProject.repositoryId(repositoryId);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not create project", e);
        }
    }

    public void enableServices(String googleProjectNumber) {
        List<String> serviceIds = Arrays.asList(
            "bigquery-json.googleapis.com",
            "firestore.googleapis.com",
            "firebaserules.googleapis.com",
            "storage-component.googleapis.com",
            "storage-api.googleapis.com"
        );
        BatchEnableServicesRequest batchRequest = new BatchEnableServicesRequest().setServiceIds(serviceIds);
        try {
            ServiceUsage serviceUsage = serviceUsage();
            ServiceUsage.Services.BatchEnable batchEnable = serviceUsage.services()
                .batchEnable("projects/" + googleProjectNumber, batchRequest);
            long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
            blockUntilServiceOperationComplete(serviceUsage, batchEnable.execute(), timeout);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not enable services", e);
        }
    }

    public void setupBilling(DataProject project) {
        BillingProfile billingProfile = profileService.getProfileById(project.getProfileId());
        billingService.assignProjectBilling(billingProfile, project);
    }

    public CloudResourceManager cloudResourceManager() throws IOException, GeneralSecurityException {
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

    public static Operation blockUntilResourceOperationComplete(
            CloudResourceManager resourceManager,
            Operation operation,
            long timeoutSeconds) throws IOException, InterruptedException{
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

    public ServiceUsage serviceUsage() throws IOException, GeneralSecurityException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        return new ServiceUsage.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(resourceConfiguration.getApplicationName())
            .build();
    }

    public static com.google.api.services.serviceusage.v1beta1.model.Operation blockUntilServiceOperationComplete(
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
