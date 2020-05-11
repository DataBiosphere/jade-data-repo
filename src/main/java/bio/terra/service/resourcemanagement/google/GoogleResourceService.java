package bio.terra.service.resourcemanagement.google;

import bio.terra.model.DatasetModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsProject;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.ProfileService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.exception.EnablePermissionsFailedException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
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
import com.google.api.services.cloudresourcemanager.model.Operation;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.api.services.cloudresourcemanager.model.SetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Status;
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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class GoogleResourceService {
    private static final Logger logger = LoggerFactory.getLogger(GoogleResourceService.class);
    private static final String ENABLED_FILTER = "state:ENABLED";
    private static final String BQ_JOB_USER_ROLE = "roles/bigquery.jobUser";


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
     * Note this method checks for the existence of the underlying cloud resource, if checkCloudResourceExists=true.
     * @param bucketResourceId
     * @param checkCloudResourceExists true to do the existence check, false to skip it
     * @return a reference to the bucket as a POJO GoogleBucketResource
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
     *
     * On entry to this method, there are 9 states along 3 main dimensions:
     * Google Bucket - exists or not
     * DR Metadata record - exists or not
     * DR Metadata lock state (only if record exists):
     *  - not locked
     *  - locked by this flight
     *  - locked by another flight
     * In addition, there is one case where it matters if we are reusing buckets or not.
     *
     * Itemizing the 9 cases:
     * CASE 1: bucket exists, record exists, record is unlocked
     *   The predominant case. We return the bucket resource
     *
     * CASE 2: bucket exists, record exists, locked by another flight
     *   We have to wait until the other flight finishes creating the bucket. Throw BucketLockFailureException.
     *   We expect the calling Step to retry on that exception.
     *
     * CASE 3: bucket exists, record exists, locked by us
     *   This flight created the bucket, but failed before we could unlock it. So, we unlock and
     *   return the bucket resource.
     *
     * CASE 4: bucket exists, no record exists, we are allowed to reuse buckets
     *   This is a common case in development where we re-use the same cloud resources over and over during
     *   testing rather than continually create and destroy them. In this case, we proceed with the
     *   try-to-create-bucket-metadata algorithm.
     *
     * CASE 5: bucket exists, no record exists, we are not reusing buckets
     *   This is the production mode and should not happen. It means we our metadata does not reflect the
     *   actual cloud resources. Throw CorruptMetadataException
     *
     * CASE 6: no bucket exists, record exists, not locked
     *   This should not happen. Throw CorruptMetadataException
     *
     * CASE 7: no bucket exists, record exists, locked by another flight
     *   We have to wait until the other flight finishes creating the bucket. Throw BucketLockFailureException.
     *   We expect the calling Step to retry on that exception.
     *
     * CASE 8: no bucket exists, record exists, locked by this flight
     *   We must have failed after creating and locking the record, but before creating the bucket.
     *   Proceed with the finish-trying-to-create-bucket algorithm
     *
     * CASE 9: no bucket exists, no record exists
     *   Proceed with try-to-create-bucket algorithm
     *
     * The algorithm to create a bucket is like a miniature flight and we implement it as a set
     * of methods that chain to make the whole algorithm:
     *  1. createMetadataRecord: create and lock the metadata record; then
     *  2. createCloudBucket: if the bucket does not exist, create it; then
     *  3. createFinish: unlock the metadata record
     * The algorithm may fail between any of those steps, so we may arrive in this method needing to
     * do some or all of those steps.
     *
     * @param bucketRequest request for a new or existing bucket
     * @param flightId flight making the request
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws CorruptMetadataException in CASE 5 and CASE 6
     * @throws BucketLockFailureException in CASE 2 and CASE 7, and sometimes case 9
     */
    public GoogleBucketResource getOrCreateBucket(GoogleBucketRequest bucketRequest, String flightId)
        throws InterruptedException {

        logger.info("application property allowReuseExistingBuckets = " + allowReuseExistingBuckets);
        String bucketName = bucketRequest.getBucketName();

        // Try to get the bucket record and the bucket object
        GoogleBucketResource googleBucketResource = resourceDao.getBucket(bucketRequest);
        Bucket bucket = getBucket(bucketRequest.getBucketName());

        // Test all of the cases
        if (bucket != null) {
            if (googleBucketResource != null) {
                String lockingFlightId = googleBucketResource.getFlightId();
                if (lockingFlightId == null) {
                    // CASE 1: everything exists and is unlocked
                    return googleBucketResource;
                }
                if (!StringUtils.equals(lockingFlightId, flightId)) {
                    // CASE 2: another flight is creating the bucket
                    throw bucketLockException(flightId);
                }
                // CASE 3: we have the flight locked, but we did all of the creating.
                return createFinish(bucket, flightId, googleBucketResource);
            } else {
                // bucket exists, but metadata record does not exist.
                if (allowReuseExistingBuckets) {
                    // CASE 4: go ahead and reuse the bucket
                    return createMetadataRecord(bucketRequest, flightId);
                } else {
                    // CASE 5:
                    throw new CorruptMetadataException(
                        "Bucket already exists, metadata out of sync with cloud state: " + bucketName);
                }
            }
        } else {
            // bucket does not exist
            if (googleBucketResource != null) {
                String lockingFlightId = googleBucketResource.getFlightId();
                if (lockingFlightId == null) {
                    // CASE 6: no bucket, but the metadata record exists unlocked
                    throw new CorruptMetadataException(
                        "Bucket does not exist, metadata out of sync with cloud state: " + bucketName);
                }
                if (!StringUtils.equals(lockingFlightId, flightId)) {
                    // CASE 7: another flight is creating the bucket
                    throw bucketLockException(flightId);
                }
                // CASE 8: this flight has the metadata locked, but didn't finish creating the bucket
                return createCloudBucket(bucketRequest, flightId, googleBucketResource);
            } else {
                // CASE 9: no bucket and no record
                return createMetadataRecord(bucketRequest, flightId);
            }
        }
    }

    private BucketLockException bucketLockException(String flightId) {
        return new BucketLockException("Bucket locked by flightId: " + flightId);
    }

    // Step 1 of creating a new bucket - create and lock the metadata record
    private GoogleBucketResource createMetadataRecord(GoogleBucketRequest bucketRequest, String flightId)
        throws InterruptedException {

        // insert a new bucket_resource row and lock it
        GoogleBucketResource googleBucketResource = resourceDao.createAndLockBucket(bucketRequest, flightId);
        if (googleBucketResource == null) {
            // We tried and failed to get the lock. So we ended up in CASE 2 after all.
            throw bucketLockException(flightId);
        }

        // this fault is used by the ResourceLockTest
        if (configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_STOP_FAULT)) {
            logger.info("BUCKET_LOCK_CONFLICT_STOP_FAULT");
            while (!configService.testInsertFault(ConfigEnum.BUCKET_LOCK_CONFLICT_CONTINUE_FAULT)) {
                logger.info("Sleeping for CONTINUE FAULT");
                TimeUnit.SECONDS.sleep(5);
            }
            logger.info("BUCKET_LOCK_CONFLICT_CONTINUE_FAULT");
        }

        return createCloudBucket(bucketRequest, flightId, googleBucketResource);
    }

    // Step 2 of creating a new bucket
    private GoogleBucketResource createCloudBucket(GoogleBucketRequest bucketRequest,
                                                   String flightId,
                                                   GoogleBucketResource googleBucketResource) {
        // If the bucket doesn't exist, create it
        Bucket bucket = getBucket(bucketRequest.getBucketName());
        if (bucket == null) {
            bucket = newBucket(bucketRequest);
        }
        return createFinish(bucket, flightId, googleBucketResource);
    }

    // Step 3 (last) of creating a new bucket
    private GoogleBucketResource createFinish(Bucket bucket,
                                              String flightId,
                                              GoogleBucketResource googleBucketResource) {
        resourceDao.unlockBucket(bucket.getName(), flightId);
        Acl.Entity owner = bucket.getOwner();
        logger.info("bucket is owned by '{}'", owner.toString());
        // TODO: ensure that the repository is the owner unless strictOwnership is false
        //  Although that might be better done in the getBucket code where we also sanity check the
        //  project the bucket is in.
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

    public GoogleProjectResource getOrCreateProject(GoogleProjectRequest projectRequest) throws InterruptedException {
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
            enableIamPermissions(googleProjectResource.getRoleIdentityMapping(), googleProjectId);
            UUID id = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(id);
        }

        return newProject(projectRequest, googleProjectId);
    }

    public void grantPoliciesBqJobUser(DatasetModel datasetModel, List<String> policyEmails)
        throws InterruptedException {

        Map<String, List<String>> policyMap = new HashMap<>();
        List<String> emails = policyEmails.stream().map((e) -> "group:" + e).collect(Collectors.toList());
        policyMap.put(BQ_JOB_USER_ROLE, emails);

        enableIamPermissions(policyMap, datasetModel.getDataProject());
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

    private GoogleProjectResource newProject(GoogleProjectRequest projectRequest, String googleProjectId)
        throws InterruptedException {

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
            enableIamPermissions(googleProjectResource.getRoleIdentityMapping(), googleProjectId);
            UUID repositoryId = resourceDao.createProject(googleProjectResource);
            return googleProjectResource.repositoryId(repositoryId);
        } catch (IOException | GeneralSecurityException e) {
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

    private void enableServices(GoogleProjectResource projectResource) throws InterruptedException {
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
        } catch (IOException | GeneralSecurityException e) {
            throw new GoogleResourceException("Could not enable services", e);
        }
    }

    private static final int RETRIES = 10;
    private static final int MAX_WAIT_SECONDS = 30;
    private static final int INITIAL_WAIT_SECONDS = 2;

    public void enableIamPermissions(Map<String, List<String>> userPermissions, String projectId)
        throws InterruptedException {

        GetIamPolicyRequest getIamPolicyRequest = new GetIamPolicyRequest();

        Exception lastException = null;
        int retryWait = INITIAL_WAIT_SECONDS;
        for (int i = 0; i < RETRIES; i++) {
            try {
                CloudResourceManager resourceManager = cloudResourceManager();
                Policy policy = resourceManager.projects()
                    .getIamPolicy(projectId, getIamPolicyRequest).execute();
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
                    .setIamPolicy(projectId, setIamPolicyRequest).execute();
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
        throw new EnablePermissionsFailedException("Cannot enable iam permissions", lastException);
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
