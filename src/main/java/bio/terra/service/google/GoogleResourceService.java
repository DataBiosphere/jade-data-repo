package bio.terra.service.google;

import bio.terra.dao.exception.google.ProjectNotFoundException;
import bio.terra.dao.google.GoogleResourceDao;
import bio.terra.flight.exception.InaccessibleBillingAccountException;
import bio.terra.metadata.google.GoogleProject;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.ProfileService;
import bio.terra.service.exception.GoogleResourceException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.Operation;
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

    @Autowired
    public GoogleResourceService(
            GoogleResourceDao resourceDao,
            ProfileService profileService,
            GoogleResourceConfiguration resourceConfiguration) {
        this.resourceDao = resourceDao;
        this.profileService = profileService;
        this.resourceConfiguration = resourceConfiguration;
    }

    public GoogleProject getProject(UUID studyId, UUID profileId) {
        try {
            return resourceDao.retrieveProjectBy("profile_id", profileId);
        } catch (ProjectNotFoundException e) {
            logger.info("Creating project since none found for profile: " + profileId);
        }
        return newProject(studyId, profileId);
    }

    public GoogleProject newProject(UUID studyId, UUID profileId) {
        // look up the profile
        BillingProfileModel profile = profileService.getProfileById(profileId);
        if (!profile.isAccessible()) {
            throw new InaccessibleBillingAccountException("The repository needs access to this billing account");
        }
        // generate a project name and save metadata
        String projectName = projectName(studyId, profileId);
        GoogleProject project = new GoogleProject()
            .studyId(studyId)
            .profileId(profileId)
            .googleProjectId(projectName);
        UUID projectId = resourceDao.createProject(project);
        project.repositoryId(projectId);

        Project requestBody = new Project()
            .setName(projectName)
            .setProjectId(projectId.toString());
        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Create request = resourceManager.projects().create(requestBody);
            Operation operation = request.execute();
            long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
            Operation completedOperation = blockUntilComplete(resourceManager, operation, timeout);
            System.out.println(completedOperation.getName());
            return project;
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not create project", e);
        }
    }

    public String projectName(UUID studyId, UUID profileId) {
        return "broad-datarepo-data-test";
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

    public static Operation blockUntilComplete(
            CloudResourceManager resourceManager,
            Operation operation,
            long timeoutSeconds) throws IOException, InterruptedException{
        long start = System.currentTimeMillis();
        final long pollInterval = 5 * 1000; // 5 seconds
        String opId = operation.getName();

        while (operation != null && !operation.getDone()) {
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
}
