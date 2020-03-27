package bio.terra.service.resourcemanagement;

import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;

public class BucketResourceLockTester implements Runnable {
    private GoogleResourceService resourceService;

    private GoogleBucketRequest bucketRequest;
    private String flightId;

    private boolean gotLockException;
    private GoogleBucketResource bucketResource;

    public BucketResourceLockTester(
        GoogleResourceService resourceService, GoogleBucketRequest bucketRequest, String flightId) {
        this.resourceService = resourceService;
        this.bucketRequest = bucketRequest;
        this.flightId = flightId;

        this.gotLockException = false;
    }

    @Override
    public void run() {
        try {
            // create the bucket and metadata
            bucketResource = resourceService.getOrCreateBucket(bucketRequest, flightId);
        } catch (BucketLockException blEx) {
            gotLockException = true;
        }
    }

    public boolean gotLockException() {
        return gotLockException;
    }

    public GoogleBucketResource getBucketResource() {
        return bucketResource;
    }
}
