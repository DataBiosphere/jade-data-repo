package bio.terra.service.resourcemanagement;

import bio.terra.app.model.GoogleRegion;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import java.util.UUID;

public class BucketResourceLockTester implements Runnable {
  private final GoogleBucketService bucketService;

  private final String bucketName;
  private final String flightId;
  private final GoogleProjectResource projectResource;
  private final DatasetBucketDao datasetBucketDao;
  private final UUID datasetId;
  private final boolean createBucketLink;
  private final GoogleRegion region;

  private boolean gotLockException;
  private GoogleBucketResource bucketResource;

  public BucketResourceLockTester(
      GoogleBucketService bucketService,
      DatasetBucketDao datasetBucketDao,
      UUID datasetId,
      String bucketName,
      GoogleProjectResource projectResource,
      GoogleRegion region,
      String flightId,
      boolean createBucketLink) {
    this.bucketService = bucketService;
    this.datasetBucketDao = datasetBucketDao;
    this.datasetId = datasetId;
    this.bucketName = bucketName;
    this.projectResource = projectResource;
    this.flightId = flightId;
    this.gotLockException = false;
    this.createBucketLink = createBucketLink;
    this.region = region;
  }

  @Override
  public void run() {
    try {
      // create the bucket and metadata
      bucketResource =
          bucketService.getOrCreateBucket(
              bucketName, projectResource, region, flightId, null, null, null);
      if (createBucketLink) {
        datasetBucketDao.createDatasetBucketLink(datasetId, bucketResource.getResourceId());
      }
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
