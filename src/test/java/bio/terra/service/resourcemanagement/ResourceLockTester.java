package bio.terra.service.resourcemanagement;

import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;

public class ResourceLockTester implements Runnable {
    private GoogleResourceService resourceService;

    private GoogleBucketRequest bucketRequest;
    private String flightId;

    private boolean gotLock;
    private GoogleBucketResource bucketResource;

    public ResourceLockTester(
        GoogleResourceService resourceService, GoogleBucketRequest bucketRequest, String flightId) {
        this.resourceService = resourceService;
        this.bucketRequest = bucketRequest;
        this.flightId = flightId;
    }

    @Override
    public void run() {
        try {
            // create the bucket and metadata
            bucketResource = resourceService.getOrCreateBucket(bucketRequest, flightId);
            gotLock = true;
        } catch (BucketLockException blEx) {
            gotLock = false;
        }
    }

    public boolean gotLock() {
        return gotLock;
    }

    public GoogleBucketResource getBucketResource() {
        return bucketResource;
    }
}
