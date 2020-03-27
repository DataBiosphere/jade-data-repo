package bio.terra.service.resourcemanagement.google;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.BucketLockFailureException;
import bio.terra.service.resourcemanagement.exception.EnablePermissionsFailedException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.ProfileService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Binding;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.api.services.cloudresourcemanager.model.SetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Status;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.Operation;
import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.api.services.serviceusage.v1beta1.model.BatchEnableServicesRequest;
import com.google.api.services.serviceusage.v1beta1.model.ListServicesResponse;
import com.google.api.services.serviceusage.v1beta1.model.Service;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class GoogleResourceService {
    private static final Logger logger = LoggerFactory.getLogger(GoogleResourceService.class);
    private static final String ENABLED_FILTER = "state:ENABLED";

    private final GoogleResourceDao resourceDao;
    private final ProfileService profileService;
    private final GoogleResourceConfiguration resourceConfiguration;
    private final GoogleBillingService billingService;
    private final GcsProjectFactory gcsProjectFactory;
    private final Environment springEnvironment;
    private final ConfigurationService configService;

    @Value("${datarepo.gcs.allowReuseExistingBuckets}") private boolean allowReuseExistingBuckets;

    @Autowired
    public GoogleResourceService(
        GoogleResourceDao resourceDao,
        ProfileService profileService,
        GoogleResourceConfiguration resourceConfiguration,
        GoogleBillingService billingService,
        GcsProjectFactory gcsProjectFactory,
        Environment springEnvironment,
        ConfigurationService configService) {
        this.resourceDao = resourceDao;
        this.profileService = profileService;
        this.resourceConfiguration = resourceConfiguration;
        this.billingService = billingService;
        this.gcsProjectFactory = gcsProjectFactory;
        this.springEnvironment = springEnvironment;
        this.configService = configService;
    }

    /**
     * Fetch an existing bucket_resource metadata row.
     * Note this method does not check for the existence of the underlying cloud resource.
     * @param bucketResourceId
     * @returna a reference to the bucket as a POJO GoogleBucketResource
     * @throws GoogleResourceNotFoundException if no bucket_resource metadata row is found
     * @throws CorruptMetadataException if the bucket_resource metadata row exists but the cloud resource does not
     */
    public GoogleBucketResource getBucketResourceById(UUID bucketResourceId, boolean checkCloudResourceExists) {
        // fetch the bucket_resource metadata row
        GoogleBucketResource bucketResource = resourceDao.retrieveBucketById(bucketResourceId);

        if (checkCloudResourceExists) {
            // throw an exception if the bucket does not already exist
            Bucket bucket = getBucket(bucketResource.getName());
            if (bucket == null) {
                throw new CorruptMetadataException(
                    "Bucket metadata exists, but bucket not found: " + bucketResource.getName());
            }
        }

        return bucketResource;
    }

    /**
     * Fetch/create a bucket cloud resource and the associated metadata in the bucket_resource table.
     * The flightId is used to lock the bucket metadata during possible creation.
     * Below is an outline of the logic flow.
     *   1. Try to insert a new bucket_resource row and lock it
     *      If the insert failed, lookup the existing row
     *   2. Check who holds the lock on the bucket_resource row
     *      a. GET case = row is unlocked. bucket has already been created
     *         Check that the bucket cloud resource exists, throw an exception if not.
     *      b. CREATE case = row is locked by this flight. need to create the bucket here
     *         Check if the bucket cloud resource exists.
     *           i. If not, create it
     *           ii. If it does exist and this is a testing environment, just use it
     *           iii. If it does exist and this is a recovery flight, just use it
     *           iv. Otherwise, the state of the cloud is out of sync with the metadat
     *               Delete anything we've created and throw an exception
     *      c. LOCKED case = the row is locked by another flight. throw a lock exception so the flight knows to retry
     * @param bucketRequest
     * @param flightId
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws CorruptMetadataException in two cases. 1) if the bucket already exists, but the metadata does not AND the
     * application property allowReuseExistingBuckets=false. 2) if the metadata exists, but the bucket does not
     */
    public GoogleBucketResource getOrCreateBucket(GoogleBucketRequest bucketRequest, String flightId) {
        // insert a new bucket_resource row and lock it
        GoogleBucketResource googleBucketResource = resourceDao.createAndLockBucket(bucketRequest, flightId);

        boolean metadataCreationFailed = (googleBucketResource == null);
        logger.debug("metadataCreationFailed = " + metadataCreationFailed);
        if (metadataCreationFailed) {
            // insert failed. lookup the existing bucket_resource row
            googleBucketResource = resourceDao.getBucket(bucketRequest);
        }

        // this fault is used by the ResourceLockTest
        if (configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT)) {
            try {
                logger.info("BUCKET_LOCK_CONFLICT_STOP_FAULT");
                while (!configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT)) {
                    logger.info("Sleeping for CONTINUE FAULT");
                    TimeUnit.SECONDS.sleep(5);
                }
                logger.info("BUCKET_LOCK_CONFLICT_CONTINUE_FAULT");
            } catch (InterruptedException intEx) {
                Thread.currentThread().interrupt();
                throw new BucketLockFailureException("Unexpected interrupt during bucket lock fault", intEx);
            }
        }

        String bucketName = bucketRequest.getBucketName();
        String lockingFlightId = googleBucketResource.getFlightId();
        logger.debug("lockingFlightId = " + lockingFlightId);
        Bucket bucket;
        if (lockingFlightId == null) {
            // GET case. the row in the bucket_resource table is unlocked. the bucket has already been created

            // throw an exception if the bucket does not already exist
            bucket = getBucket(bucketName);
            if (bucket == null) {
                throw new CorruptMetadataException("Bucket not found: " + bucketName);
            }
        } else if (lockingFlightId.equals(flightId)) {
            // CREATE case. the row in the bucket_resource table is locked by this flight. we need to create it here

            // check if the bucket already exists
            bucket = getBucket(bucketName);
            if (bucket == null) {
                // bucket DOES NOT EXIST. create it and unlock
                bucket = newBucket(bucketRequest);
                resourceDao.unlockBucket(bucketName, flightId);
            } else {
                // bucket EXISTS. if this is a recovery flight or testing environment, use it. otherwise throw exception
                // testing environments should set datarepo.gcs.allowReuseExistingBuckets=true in application.properties
                logger.info("application property allowReuseExistingBuckets = " + allowReuseExistingBuckets);
                if (metadataCreationFailed || allowReuseExistingBuckets) {
                    logger.debug(String.format("bucket already exists, using anyway: %s", bucketName));
                    resourceDao.unlockBucket(bucketName, flightId);
                } else {
                    resourceDao.deleteBucketMetadata(bucketName, flightId);
                    throw new CorruptMetadataException(
                        String.format("Bucket already exists, metadata out of sync with cloud state: %s", bucketName));
                }
            }
        } else {
            // LOCKED case. the row in the bucket_resource table is locked by another flight. throw a lock exception
            throw new BucketLockException("The bucket is being created by another flight.");
        }

        Acl.Entity owner = bucket.getOwner();
        logger.info("bucket is owned by '{}'", owner.toString());
        // TODO: ensure that the repository is the owner unless strictOwnership is false

        // return the resource object
        return googleBucketResource;
    }

    /**
     * Update the bucket_resource metadata table to match the state of the underlying cloud.
     *    - If the bucket exists, then the metadata row should also exist and be unlocked.
     *    - If the bucket does not exist, then the metadata row should not exist.
     * If the metadata row is locked, then only the locking flight can unlock or delete the row.
     * @param bucketName
     * @param flightId
     */
    public void updateBucketMetadata(String bucketName, String flightId) {
        // check if the bucket already exists
        Bucket existingBucket = getBucket(bucketName);
        if (existingBucket != null) {
            // bucket EXISTS. unlock the metadata row
            resourceDao.unlockBucket(bucketName, flightId);
        } else {
            // bucket DOES NOT EXIST. delete the metadata row
            resourceDao.deleteBucketMetadata(bucketName, flightId);
        }
    }

    /**
     * Create a new bucket cloud resource.
     * Note this method does not create any associated metadata in the bucket_resource table.
     * @param bucketRequest
     * @return a reference to the bucket as a GCS Bucket object
     */
    private Bucket newBucket(GoogleBucketRequest bucketRequest) {
        String bucketName = bucketRequest.getBucketName();
        GoogleProjectResource projectResource = bucketRequest.getGoogleProjectResource();
        String googleProjectId = projectResource.getGoogleProjectId();
        GcsProject gcsProject = gcsProjectFactory.get(googleProjectId);
        BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName)
            //.setRequesterPays()
            // See here for possible values: http://g.co/cloud/storage/docs/storage-classes
            .setStorageClass(StorageClass.REGIONAL)
            .setLocation(bucketRequest.getRegion())
            .build();
        // the project will have been created before this point, so no need to fetch it
        logger.info("Creating bucket '{}' in project '{}'", bucketName, googleProjectId);
        return gcsProject.getStorage().create(bucketInfo);
    }

    /**
     * Fetch an existing bucket cloud resource.
     * Note this method does not check any associated metadata in the bucket_resource table.
     * @param bucketName
     * @return a reference to the bucket as a GCS Bucket object, null if not found
     */
    private Bucket getBucket(String bucketName) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        try {
            return storage.get(bucketName);
        } catch (StorageException e) {
            throw new GoogleResourceException("Could not check bucket existence", e);
        }
    }

    /**
     * Setter for allowReuseExistingBuckets property.
     * This property should be set in application.properties (or the appropriate one for your environment).
     * This setter should only be used by tests to programmatically modify the flag to test behavior without running
     * with two different properties files.
     * @param newValue
     * @return this
     */
    public GoogleResourceService setAllowReuseExistingBuckets(boolean newValue) {
        allowReuseExistingBuckets = newValue;
        return this;
    }

    /**
     * Getter for allowReuseExistingBuckets property.
     * This property should be read from application.properties (or the appropriate one for your environment).
     * This getter should only be used by tests to programmatically modify the flag to test behavior without running
     * with two different properties files.
     * @return boolean property value
     */
    public boolean getAllowReuseExistingBuckets() {
        return allowReuseExistingBuckets;
    }

    public GoogleProjectResource getOrCreateProject(GoogleProjectRequest projectRequest) {
        // Naive: this implements a 1-project-per-profile approach. If there is already a Google project for this
        // profile we will look up the project by id, otherwise we will generate one and look it up
        String googleProjectId = projectRequest.getProjectId();
        try {
            return resourceDao.retrieveProjectByGoogleProjectId(googleProjectId);
        } catch (GoogleResourceNotFoundException e) {
            logger.info("no project resource found for projectId: {}", googleProjectId);
        }

        // it's possible that the project exists already but it is not stored in the metadata table
        // TODO: ensure that the ownership, read/write perms are correct!
        Project existingProject = getProject(googleProjectId);
        if (existingProject != null) {
            GoogleProjectResource googleProjectResource = new GoogleProjectResource(projectRequest)
                .googleProjectId(googleProjectId)
                .googleProjectNumber(existingProject.getProjectNumber().toString());
            enableServices(googleProjectResource);
            enableIamPermissions(googleProjectResource);
            UUID id = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(id);
        }

        return newProject(projectRequest, googleProjectId);
    }

    public GoogleProjectResource getProjectResourceById(UUID id) {
        return resourceDao.retrieveProjectById(id);
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

    private GoogleProjectResource newProject(GoogleProjectRequest projectRequest, String googleProjectId) {
        BillingProfile profile = profileService.getProfileById(projectRequest.getProfileId());
        logger.info("creating a new project: {}", projectRequest.getProjectId());
        if (!profile.isAccessible()) {
            throw new InaccessibleBillingAccountException("The repository needs access to this billing account " +
                "in order to create: " + googleProjectId);
        }

        // projects created by service accounts must live under a parent resource (either a folder or an organization)
        ResourceId parentResource = new ResourceId()
            .setType(resourceConfiguration.getParentResourceType())
            .setId(resourceConfiguration.getParentResourceId());
        Project requestBody = new Project()
            .setName(googleProjectId)
            .setProjectId(googleProjectId)
            .setParent(parentResource);
        try {
            // kick off a project create request and poll until it is done
            CloudResourceManager resourceManager = cloudResourceManager();
            CloudResourceManager.Projects.Create request = resourceManager.projects().create(requestBody);
            Operation operation = request.execute();
            long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
            blockUntilResourceOperationComplete(resourceManager, operation, timeout);
            // it should be retrievable once the create operation is complete
            Project project = getProject(googleProjectId);
            if (project == null) {
                throw new GoogleResourceException("Could not get project after creation");
            }
            String googleProjectNumber = project.getProjectNumber().toString();
            GoogleProjectResource googleProjectResource = new GoogleProjectResource(projectRequest)
                // TODO: handle case where we get a different project id than requested
                .googleProjectId(googleProjectId)
                .googleProjectNumber(googleProjectNumber);
            setupBilling(googleProjectResource);
            enableServices(googleProjectResource);
            enableIamPermissions(googleProjectResource);
            UUID repositoryId = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(repositoryId);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not create project", e);
        }
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
    public void deleteProjectResource(UUID resourceId) {
        GoogleProjectResource projectResource = resourceDao.retrieveProjectById(resourceId);
        deleteGoogleProject(projectResource.getGoogleProjectId());
        resourceDao.deleteProject(resourceId);
    }

    private void enableServices(GoogleProjectResource projectResource) {
        BatchEnableServicesRequest batchRequest = new BatchEnableServicesRequest()
            .setServiceIds(projectResource.getServiceIds());
        try {
            ServiceUsage serviceUsage = serviceUsage();
            String projectNumberString = "projects/" + projectResource.getGoogleProjectNumber();
            logger.info("trying to get services for {} ({})", projectNumberString,
                projectResource.getGoogleProjectId());
            ServiceUsage.Services.List list = serviceUsage.services()
                .list(projectNumberString)
                .setFilter(ENABLED_FILTER);
            ListServicesResponse listServicesResponse = list.execute();
            logger.info("found: " + String.join(", ", projectResource.getServiceIds()));
            List<String> services = projectResource.getServiceIds()
                .stream()
                .map(s -> String.format("%s/services/%s", projectNumberString, s))
                .collect(Collectors.toList());
            List<Service> serviceList = listServicesResponse.getServices();
            List<String> actualServiceNames = Collections.emptyList();
            if (serviceList != null) {
                actualServiceNames = serviceList
                    .stream()
                    .map(s -> s.getName())
                    .collect(Collectors.toList());
            }

            if (actualServiceNames.containsAll(services)) {
                logger.info("project already has the right resources enabled, skipping");
            } else {
                logger.info("project does not have all resources enabled");
                ServiceUsage.Services.BatchEnable batchEnable = serviceUsage.services()
                    .batchEnable(projectNumberString, batchRequest);
                long timeout = resourceConfiguration.getProjectCreateTimeoutSeconds();
                blockUntilServiceOperationComplete(serviceUsage, batchEnable.execute(), timeout);
            }
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            throw new GoogleResourceException("Could not enable services", e);
        }
    }

    public void enableIamPermissions(GoogleProjectResource projectResource) {
        Map<String, List<String>> userPermissions = projectResource.getRoleIdentityMapping();
        GetIamPolicyRequest getIamPolicyRequest = new GetIamPolicyRequest();

        try {
            CloudResourceManager resourceManager = cloudResourceManager();
            Policy policy = resourceManager.projects()
                .getIamPolicy(projectResource.getGoogleProjectId(), getIamPolicyRequest).execute();
            List<Binding> bindingsList = policy.getBindings();

            for (Map.Entry<String, List<String>> entry : userPermissions.entrySet()) {
                Binding binding = new Binding()
                    .setRole(entry.getKey())
                    .setMembers(entry.getValue());
                bindingsList.add(binding);
            }

            policy.setBindings(bindingsList);
            SetIamPolicyRequest setIamPolicyRequest = new SetIamPolicyRequest().setPolicy(policy);
            resourceManager.projects()
                .setIamPolicy(projectResource.getGoogleProjectId(), setIamPolicyRequest).execute();
        } catch (IOException | GeneralSecurityException ex) {
            throw new EnablePermissionsFailedException("Cannot enable iam permissions", ex);
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
            credential = credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }

        return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(resourceConfiguration.getApplicationName())
            .build();
    }

    /**
     * Poll the resource manager api until an operation completes. It is possible to hit quota issues here, so the
     * timeout is set to 10 seconds.
     * @param resourceManager service instance
     * @param operation has an id for us to use in the check
     * @param timeoutSeconds how many seconds before we give up
     * @return a completed operation
     */
    private static Operation blockUntilResourceOperationComplete(
            CloudResourceManager resourceManager,
            Operation operation,
            long timeoutSeconds) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final long pollInterval = 10 * 1000; // 10 seconds
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
            credential = credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }

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
