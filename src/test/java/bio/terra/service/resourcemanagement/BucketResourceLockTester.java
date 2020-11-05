package bio.terra.service.resourcemanagement;

import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;

public class BucketResourceLockTester implements Runnable {
    private final GoogleBucketService bucketService;

    private final String bucketName;
    private final String flightId;
    private final GoogleProjectResource projectResource;

    private boolean gotLockException;
    private GoogleBucketResource bucketResource;

    public BucketResourceLockTester(GoogleBucketService bucketService,
                                    String bucketName,
                                    GoogleProjectResource projectResource,
                                    String flightId) {
        this.bucketService = bucketService;
        this.bucketName = bucketName;
        this.projectResource = projectResource;
        this.flightId = flightId;
        this.gotLockException = false;
    }

    @Override
    public void run() {
        try {
            // create the bucket and metadata
            bucketResource = bucketService.getOrCreateBucket(bucketName, projectResource, flightId);
        } catch (BucketLockException blEx) {
            gotLockException = true;
        } catch (InterruptedException e) {
            gotLockException = false;
        }
    }

    public boolean gotLockException() {
        return gotLockException;
    }

    public GoogleBucketResource getBucketResource() {
        return bucketResource;
    }
}
