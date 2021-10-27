package bio.terra.service.filedata.google.gcs;

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;
import static bio.terra.service.filedata.DrsService.getLastNameFromPath;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.AclUtils;
import bio.terra.common.FutureUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.exception.PdaoFileCopyException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.exception.BlobAccessNotAuthorizedException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamRole;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class GcsPdao implements CloudFileReader {
  private static final Logger logger = LoggerFactory.getLogger(GcsPdao.class);

  private static final List<String> GCS_VERIFICATION_SCOPES =
      List.of(
          "openid", "email", "profile", "https://www.googleapis.com/auth/devstorage.full_control");

  private final GcsProjectFactory gcsProjectFactory;
  private final ResourceService resourceService;
  private final FireStoreDao fileDao;
  private final ConfigurationService configurationService;
  private final ExecutorService executor;
  private final PerformanceLogger performanceLogger;
  private final IamProviderInterface iamClient;
  private final Environment environment;

  @Autowired
  public GcsPdao(
      GcsProjectFactory gcsProjectFactory,
      ResourceService resourceService,
      FireStoreDao fileDao,
      ConfigurationService configurationService,
      @Qualifier("performanceThreadpool") ExecutorService executor,
      PerformanceLogger performanceLogger,
      IamProviderInterface iamClient,
      Environment environment) {
    this.gcsProjectFactory = gcsProjectFactory;
    this.resourceService = resourceService;
    this.fileDao = fileDao;
    this.configurationService = configurationService;
    this.executor = executor;
    this.performanceLogger = performanceLogger;
    this.iamClient = iamClient;
    this.environment = environment;
  }

  public Storage storageForBucket(GoogleBucketResource bucketResource) {
    return gcsProjectFactory.getStorage(bucketResource.projectIdForBucket());
  }

  /**
   * Get all the lines from any files matching the blobUrl as a Stream, including wildcarded paths
   *
   * <p>It is important that the stream returned by this method be guaranteed to closed. Since this
   * is a stream from IO, we need to make sure that the handle is closed. This can be done by
   * wrapping the stream in a try-block once the stream is used for a terminal operation.
   *
   * @param blobUrl blobUrl to files, or blobUrl including wildcard referring to many files
   * @param cloudEncapsulationId Project ID to use for storage service in case of requester pays
   *     bucket
   * @return All of the lines from all of the files matching the blobUrl, as a Stream
   */
  @Override
  public Stream<String> getBlobsLinesStream(String blobUrl, String cloudEncapsulationId) {
    Storage storage = gcsProjectFactory.getStorage(cloudEncapsulationId);
    int lastWildcard = blobUrl.lastIndexOf("*");
    String prefixPath = lastWildcard >= 0 ? blobUrl.substring(0, lastWildcard) : blobUrl;
    return listGcsFiles(prefixPath, cloudEncapsulationId, storage)
        .flatMap(blob -> getBlobLinesStream(blob, cloudEncapsulationId, storage));
  }

  @SuppressFBWarnings("OS_OPEN_STREAM")
  private static Stream<String> getBlobLinesStream(Blob blob, String projectId, Storage storage) {
    logger.info(String.format("Reading lines from %s", GcsUriUtils.getGsPathFromBlob(blob)));
    var reader = storage.reader(blob.getBlobId(), Storage.BlobSourceOption.userProject(projectId));
    var channelReader = Channels.newReader(reader, StandardCharsets.UTF_8);
    var bufferedReader = new BufferedReader(channelReader);
    return bufferedReader.lines();
  }

  private Stream<Blob> listGcsFiles(String path, String projectId, Storage storage) {
    BlobId locator = GcsUriUtils.parseBlobUri(path);
    Iterable<Blob> blobs =
        storage
            .list(
                locator.getBucket(),
                Storage.BlobListOption.prefix(locator.getName()),
                Storage.BlobListOption.userProject(projectId))
            .iterateAll();
    return StreamSupport.stream(blobs.spliterator(), false);
  }

  /**
   * Write String to a GCS file
   *
   * @param path gs path to write the lines to
   * @param contentsToWrite contents to write to file
   * @param projectId project for billing
   */
  public void writeStreamToCloudFile(
      String path, Stream<String> contentsToWrite, String projectId) {
    logger.info("Writing contents to {}", path);
    Storage storage = gcsProjectFactory.getStorage(projectId);
    var blob = getBlobFromGsPath(storage, path, projectId);
    var newLine = "\n".getBytes(StandardCharsets.UTF_8);
    try (var writer = blob.writer(Storage.BlobWriteOption.userProject(projectId))) {
      contentsToWrite.forEach(
          s -> {
            try {
              writer.write(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
              writer.write(ByteBuffer.wrap(newLine));
            } catch (IOException e) {
              throw new GoogleResourceException(
                  String.format("Could not write to GCS file at %s. Line: %s", path, s), e);
            }
          });
    } catch (IOException ex) {
      throw new GoogleResourceException(
          String.format("Could not write to GCS file at %s", path), ex);
    }
  }

  /**
   * Create a file in GCS
   *
   * @param path path for the new file to be created at
   * @param projectId project id for billing
   * @return the Blob of the created file
   */
  public Blob createGcsFile(String path, String projectId) {
    Storage storage = gcsProjectFactory.getStorage(projectId);
    logger.info("Creating GCS file at {}", path);
    BlobId locator = GcsUriUtils.parseBlobUri(path);
    return storage.create(
        BlobInfo.newBuilder(locator).build(), Storage.BlobTargetOption.userProject(projectId));
  }

  /**
   * Given a list a source paths, validate that the specified user has a pat service account with
   * read permissions
   *
   * @param sourcePaths A list of gs:// formatted paths
   * @param user An authenticed user
   * @throws BlobAccessNotAuthorizedException if the user does not have an authorized pet
   */
  public void validateUserCanRead(List<String> sourcePaths, AuthenticatedUserRequest user) {
    // If the connected profile is used, skip this check since we don't specify users when mocking
    // requests
    if (List.of(environment.getActiveProfiles()).contains("connectedtest")) {
      return;
    }
    // Obtain a token for the user's pet service account that can verify that it is allowed to read
    String token;
    try {
      token = iamClient.getPetToken(user, GCS_VERIFICATION_SCOPES);
    } catch (InterruptedException e) {
      throw new PdaoException("Error obtaining a pet service account token");
    }

    Storage storageAsPet =
        StorageOptions.newBuilder()
            .setCredentials(OAuth2Credentials.create(new AccessToken(token, null)))
            .build()
            .getService();

    Set<String> buckets =
        sourcePaths.stream()
            .map(GcsUriUtils::parseBlobUri)
            .map(BlobId::getBucket)
            .collect(Collectors.toSet());
    for (String bucket : buckets) {
      List<Boolean> permissions =
          storageAsPet.testIamPermissions(bucket, List.of("storage.objects.get"));

      if (!permissions.equals(List.of(true))) {
        throw new BlobAccessNotAuthorizedException(
            String.format(
                "Accessing bucket %s is not authorized. Please be sure to grant \"Storage Object Viewer\" permissions to the TDR service account and your Terra proxy user group",
                bucket));
      }
    }
  }

  public List<BlobId> listGcsIngestBlobs(String path, String projectId) {
    int lastWildcard = path.lastIndexOf("*");
    if (lastWildcard >= 0) {
      Storage storage = gcsProjectFactory.getStorage(projectId);
      String prefixPath = path.substring(0, lastWildcard);
      return listGcsFiles(prefixPath, projectId, storage)
          .map(BlobInfo::getBlobId)
          .collect(Collectors.toList());
    } else {
      return List.of(GcsUriUtils.parseBlobUri(path));
    }
  }

  public void copyGcsFile(BlobId from, BlobId to, String projectId) {
    logger.info("Copying GCS file from {} to {}", from, to);
    Storage storage = gcsProjectFactory.getStorage(projectId);
    storage.get(from).copyTo(to, Blob.BlobSourceOption.userProject(projectId));
  }

  public FSFileInfo copyFile(
      Dataset dataset,
      FileLoadModel fileLoadModel,
      String fileId,
      GoogleBucketResource bucketResource) {

    try {
      Storage storage = storageForBucket(bucketResource);
      String targetProjectId = bucketResource.projectIdForBucket();
      Blob sourceBlob = getBlobFromGsPath(storage, fileLoadModel.getSourcePath(), targetProjectId);

      // Read the leaf node of the source file to use as a way to name the file we store
      String sourceFileName = getLastNameFromPath(sourceBlob.getName());
      // Our path is /<dataset-id>/<file-id>/<source-file-name>
      String targetPath = dataset.getId().toString() + "/" + fileId + "/" + sourceFileName;

      // The documentation is vague whether or not it is important to copy by chunk. One set of
      // examples does it and another doesn't.
      //
      // I have been seeing timeouts and I think they are due to particularly large files,
      // so I exported the timeouts to application.properties to allow for tuning
      // and I am changing this to copy chunks.
      //
      // Specify the target project of the target bucket as the payor if the source is requester
      // pays.
      CopyWriter writer =
          sourceBlob.copyTo(
              BlobId.of(bucketResource.getName(), targetPath),
              Blob.BlobSourceOption.userProject(targetProjectId));
      while (!writer.isDone()) {
        writer.copyChunk();
      }
      Blob targetBlob = writer.getResult();

      // MD5 is computed per-component. So if there are multiple components, the MD5 here is
      // not useful for validating the contents of the file on access. Therefore, we only
      // return the MD5 if there is only a single component. For more details,
      // see https://cloud.google.com/storage/docs/hashes-etags
      Integer componentCount = targetBlob.getComponentCount();
      String checksumMd5 = null;
      if (componentCount == null || componentCount == 1) {
        checksumMd5 = targetBlob.getMd5ToHexString();
      }

      // Grumble! It is not documented what the meaning of the Long is.
      // From poking around I think it is a standard POSIX milliseconds since Jan 1, 1970.
      Instant createTime = Instant.ofEpochMilli(targetBlob.getCreateTime());

      String gspath = String.format("gs://%s/%s", bucketResource.getName(), targetPath);

      return new FSFileInfo()
          .fileId(fileId)
          .createdDate(createTime.toString())
          .cloudPath(gspath)
          .checksumCrc32c(targetBlob.getCrc32cToHexString())
          .checksumMd5(checksumMd5)
          .size(targetBlob.getSize())
          .bucketResourceId(bucketResource.getResourceId().toString());

    } catch (StorageException ex) {
      // For now, we assume that the storage exception is caused by bad input (the file copy
      // exception
      // derives from BadRequestException). I think there are several cases here. We might need to
      // retry
      // for flaky google case or we might need to bail out if access is denied.
      throw new PdaoFileCopyException("File ingest failed", ex);
    }
  }

  // Three flavors of deleteFileMetadata
  // 1. for undo file ingest - it gets the bucket path from the dataset and file id
  // 2. for delete file flight - it gets bucket path from the gspath
  // 3. for delete file consumer for deleting all files - it gets bucket path
  //    from gspath in the fireStoreFile

  public boolean deleteFileById(
      Dataset dataset, String fileId, String fileName, GoogleBucketResource bucketResource) {
    String bucketPath = dataset.getId().toString() + "/" + fileId + "/" + fileName;
    return deleteWorker(bucketResource, bucketPath);
  }

  public boolean deleteFileByGspath(String inGspath, GoogleBucketResource bucketResource) {
    if (inGspath != null) {
      String bucketPath = extractFilePathInBucket(inGspath, bucketResource.getName());
      return deleteWorker(bucketResource, bucketPath);
    }
    return false;
  }

  // Consumer method for deleting GCS files driven from a scan over the firestore files
  public void deleteFile(FireStoreFile fireStoreFile) {
    if (fireStoreFile != null) {
      GoogleBucketResource bucketResource =
          resourceService.lookupBucket(fireStoreFile.getBucketResourceId());
      deleteFileByGspath(fireStoreFile.getGspath(), bucketResource);
    }
  }

  private boolean deleteWorker(GoogleBucketResource bucketResource, String bucketPath) {
    GcsProject gcsProject =
        gcsProjectFactory.get(bucketResource.getProjectResource().getGoogleProjectId());
    Storage storage = gcsProject.getStorage();
    Blob blob = storage.get(BlobId.of(bucketResource.getName(), bucketPath));
    if (blob != null) {
      return blob.delete();
    }
    logger.warn("{} was not found and so deletion was skipped", bucketPath);
    return false;
  }

  private enum AclOp {
    ACL_OP_CREATE,
    ACL_OP_DELETE
  }

  public void setAclOnFiles(Dataset dataset, List<String> fileIds, Map<IamRole, String> policies)
      throws InterruptedException {
    fileAclOp(AclOp.ACL_OP_CREATE, dataset, fileIds, policies);
  }

  public void removeAclOnFiles(Dataset dataset, List<String> fileIds, Map<IamRole, String> policies)
      throws InterruptedException {
    fileAclOp(AclOp.ACL_OP_DELETE, dataset, fileIds, policies);
  }

  public static Blob getBlobFromGsPath(Storage storage, String gspath, String targetProjectId) {
    BlobId locator = GcsUriUtils.parseBlobUri(gspath);

    // Provide the project of the destination of the file copy to pay if the
    // source bucket is requester pays.
    Blob sourceBlob = storage.get(locator, Storage.BlobGetOption.userProject(targetProjectId));
    if (sourceBlob == null) {
      throw new PdaoSourceFileNotFoundException("Source file not found: '" + gspath + "'");
    }

    return sourceBlob;
  }

  public static String getBlobContents(Storage storage, String projectId, BlobInfo blobInfo) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    var contents = blob.getContent(Blob.BlobSourceOption.userProject(projectId));
    return new String(contents, StandardCharsets.UTF_8);
  }

  public static int writeBlobContents(
      Storage storage, String projectId, BlobInfo blobInfo, String contents) {
    var blob = storage.get(blobInfo.getBlobId(), Storage.BlobGetOption.userProject(projectId));
    try (var writer = blob.writer(Storage.BlobWriteOption.userProject(projectId))) {
      return writer.write(ByteBuffer.wrap(contents.getBytes(StandardCharsets.UTF_8)));
    } catch (IOException ex) {
      throw new GoogleResourceException(
          String.format(
              "Could not write to GCS file at %s", GcsUriUtils.getGsPathFromBlob(blobInfo)),
          ex);
    }
  }

  public static int writeBlobContents(
      Storage storage, String projectId, String gsPath, String contents) {
    return writeBlobContents(
        storage, projectId, getBlobFromGsPath(storage, gsPath, projectId), contents);
  }

  public GoogleRegion getRegionForFile(String path, String googleProjectId) {
    Storage storage = gcsProjectFactory.getStorage(googleProjectId);
    BlobId locator = GcsUriUtils.parseBlobUri(path);
    Bucket bucket =
        storage.get(locator.getBucket(), Storage.BucketGetOption.userProject(googleProjectId));
    return GoogleRegion.fromValue(bucket.getLocation());
  }

  private void fileAclOp(
      AclOp op, Dataset dataset, List<String> fileIds, Map<IamRole, String> policies)
      throws InterruptedException {

    // Build all the groups that need to get read access to the files
    List<Acl.Group> groups = new LinkedList<>();
    groups.add(new Acl.Group(policies.get(IamRole.READER)));
    groups.add(new Acl.Group(policies.get(IamRole.CUSTODIAN)));
    groups.add(new Acl.Group(policies.get(IamRole.STEWARD)));

    // build acls if necessary
    List<Acl> acls = new LinkedList<>();
    if (op == AclOp.ACL_OP_CREATE) {
      for (Acl.Group group : groups) {
        acls.add(Acl.newBuilder(group, Acl.Role.READER).build());
      }
    }

    // Cache buckets by bucket resource ID
    final Map<String, GoogleBucketResource> bucketCache = new ConcurrentHashMap<>();

    int batchSize = configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE);
    List<List<String>> batches = ListUtils.partition(fileIds, batchSize);
    logger.info(
        "operation {} on {} file ids, in {} batches of {}",
        op.name(),
        fileIds.size(),
        batches.size(),
        batchSize);

    String retrieveTimer = performanceLogger.timerStart();
    int count = 0;
    for (List<String> batch : batches) {
      logger.info("operation {} batch {}", op.name(), count);
      count++;

      List<FSFile> files = fileDao.batchRetrieveById(dataset, batch, 0);

      try (Stream<FSFile> stream = files.stream()) {
        List<Future<FSFile>> futures =
            stream
                .distinct()
                .map(
                    file -> executor.submit(performAclCommand(bucketCache, file, op, acls, groups)))
                .collect(Collectors.toList());

        FutureUtils.waitFor(futures);
      }
    }

    performanceLogger.timerEndAndLog(
        retrieveTimer,
        dataset.getId().toString(), // not a flight, so no job id
        this.getClass().getName(),
        "gcsPdao.performAclCommands");
  }

  /** Perform the ACL setting commands on a specific file. */
  private Callable<FSFile> performAclCommand(
      final Map<String, GoogleBucketResource> bucketCache,
      final FSFile file,
      final AclOp op,
      final List<Acl> acls,
      final List<Acl.Group> groups) {
    Callable<FSFile> aclUpdate =
        () -> {
          try {
            // Cache the bucket resources to avoid repeated database lookups.
            // Synchronizing this block since this gets called with a potentially high degree of
            // concurrency.
            // Minimal overhead to lock here since 99% of the time this will be a simple map lookup
            final GoogleBucketResource bucketForFile;
            synchronized (bucketCache) {
              bucketForFile =
                  bucketCache.computeIfAbsent(
                      file.getBucketResourceId(),
                      k -> resourceService.lookupBucket(file.getBucketResourceId()));
            }
            final Storage storage = storageForBucket(bucketForFile);
            final String bucketPath =
                extractFilePathInBucket(file.getCloudPath(), bucketForFile.getName());
            final BlobId blobId = BlobId.of(bucketForFile.getName(), bucketPath);
            switch (op) {
              case ACL_OP_CREATE:
                for (Acl acl : acls) {
                  storage.createAcl(blobId, acl);
                }
                break;
              case ACL_OP_DELETE:
                for (Acl.Group group : groups) {
                  storage.deleteAcl(blobId, group);
                }
                break;
            }
            return file;
          } catch (StorageException ex) {
            throw new AclUtils.AclRetryException(ex.getMessage(), ex, ex.getReason());
          }
        };
    return () -> AclUtils.aclUpdateRetry(aclUpdate);
  }

  /**
   * Extract the path portion (everything after the bucket name and it's trailing slash) of a gs
   * path.
   */
  private static String extractFilePathInBucket(final String path, final String bucketName) {
    return StringUtils.removeStart(path, String.format("gs://%s/", bucketName));
  }
}
