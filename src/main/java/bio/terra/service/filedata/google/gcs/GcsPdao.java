package bio.terra.service.filedata.google.gcs;

import bio.terra.common.exception.PdaoException;
import bio.terra.common.exception.PdaoFileCopyException;
import bio.terra.common.exception.PdaoInvalidUriException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.iam.IamRole;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;
import static bio.terra.service.filedata.DrsService.getLastNameFromPath;

@Component
public class GcsPdao {
    private static final Logger logger = LoggerFactory.getLogger(GcsPdao.class);

    private final GcsProjectFactory gcsProjectFactory;
    private final DataLocationService dataLocationService;
    private final FireStoreDao fileDao;
    private final ApplicationContext applicationContext;
    private final ConfigurationService configurationService;

    @Autowired
    public GcsPdao(
        GcsProjectFactory gcsProjectFactory,
        DataLocationService dataLocationService,
        FireStoreDao fileDao,
        ApplicationContext applicationContext,
        ConfigurationService configurationService) {
        this.gcsProjectFactory = gcsProjectFactory;
        this.dataLocationService = dataLocationService;
        this.fileDao = fileDao;
        this.applicationContext = applicationContext;
        this.configurationService = configurationService;
    }

    public Storage storageForBucket(GoogleBucketResource bucketResource) {
        return gcsProjectFactory.getStorage(bucketResource.projectIdForBucket());
    }
    public FSFileInfo copyFile(Dataset dataset,
                               FileLoadModel fileLoadModel,
                               String fileId,
                               GoogleBucketResource bucketResource) {

        try {
            Storage storage = storageForBucket(bucketResource);
            String targetProjectId = bucketResource.projectIdForBucket();
            Blob sourceBlob = getBlobFromGsPath(storage, fileLoadModel.getSourcePath(), targetProjectId);

            // Read the leaf node of the source file to use as a way to sname the file we store
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
            // Specify the target project of the target bucket as the payor if the source is requester pays.
            CopyWriter writer = sourceBlob.copyTo(
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

            URI gspath = new URI("gs",
                bucketResource.getName(),
                "/" + targetPath,
                null,
                null);

            FSFileInfo fsFileInfo = new FSFileInfo()
                .fileId(fileId)
                .createdDate(createTime.toString())
                .gspath(gspath.toString())
                .checksumCrc32c(targetBlob.getCrc32cToHexString())
                .checksumMd5(checksumMd5)
                .size(targetBlob.getSize())
                .bucketResourceId(bucketResource.getResourceId().toString());

            return fsFileInfo;
        } catch (StorageException ex) {
            // For now, we assume that the storage exception is caused by bad input (the file copy exception
            // derives from BadRequestException). I think there are several cases here. We might need to retry
            // for flaky google case or we might need to bail out if access is denied.
            throw new PdaoFileCopyException("File ingest failed", ex);
        } catch (URISyntaxException ex) {
            throw new PdaoException("Bad URI of our own making", ex);
        }
    }

    // Three flavors of deleteFileMetadata
    // 1. for undo file ingest - it gets the bucket path from the dataset and file id
    // 2. for delete file flight - it gets bucket path from the gspath
    // 3. for delete file consumer for deleting all files - it gets bucket path
    //    from gspath in the fireStoreFile

    public boolean deleteFileById(Dataset dataset,
                                  String fileId,
                                  String fileName,
                                  GoogleBucketResource bucketResource) {
        String bucketPath = dataset.getId().toString() + "/" + fileId + "/" + fileName;
        return deleteWorker(bucketResource, bucketPath);
    }

    public boolean deleteFileByGspath(String inGspath, GoogleBucketResource bucketResource) {
        if (inGspath != null) {
            URI uri = URI.create(inGspath);
            String bucketPath = StringUtils.removeStart(uri.getPath(), "/");
            return deleteWorker(bucketResource, bucketPath);
        }
        return false;
    }

    // Consumer method for deleting GCS files driven from a scan over the firestore files
    public void deleteFile(FireStoreFile fireStoreFile) {
        if (fireStoreFile != null) {
            GoogleBucketResource bucketResource = dataLocationService.lookupBucket(fireStoreFile.getBucketResourceId());
            deleteFileByGspath(fireStoreFile.getGspath(), bucketResource);
        }
    }

    private boolean deleteWorker(GoogleBucketResource bucketResource, String bucketPath) {
        GcsProject gcsProject = gcsProjectFactory.get(bucketResource.getProjectResource().getGoogleProjectId());
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

    private static final String GS_PROTOCOL = "gs://";
    private static final String GS_BUCKET_PATTERN = "[a-z0-9_.\\-]{3,222}";

    public static Blob getBlobFromGsPath(Storage storage, String gspath, String targetProjectId) {
        if (!StringUtils.startsWith(gspath, GS_PROTOCOL)) {
            throw new PdaoInvalidUriException("Path is not a gs path: '" + gspath + "'");
        }

        String noGsUri = StringUtils.substring(gspath, GS_PROTOCOL.length());
        String sourceBucket = StringUtils.substringBefore(noGsUri, "/");
        String sourcePath = StringUtils.substringAfter(noGsUri, "/");

        /*
         * GCS bucket names must:
         *   1. Match the regex '[a-z0-9_.\-]{3,222}
         *   2. With a max of 63 characters between each '.'
         *
         * https://cloud.google.com/storage/docs/naming-buckets#requirements
         */
        if (!sourceBucket.matches(GS_BUCKET_PATTERN)) {
            throw new PdaoInvalidUriException("Invalid bucket name in gs path: '" + gspath + "'");
        }
        String[] bucketComponents = sourceBucket.split("\\.");
        for (String component : bucketComponents) {
            if (component.length() > 63) {
                throw new PdaoInvalidUriException(
                    "Component name '" + component + "' too long in gs path: '" + gspath + "'");
            }
        }

        if (sourcePath.isEmpty()) {
            throw new PdaoInvalidUriException("Missing object name in gs path: '" + gspath + "'");
        }

        // Provide the project of the destination of the file copy to pay if the
        // source bucket is requester pays.
        Blob sourceBlob = storage.get(BlobId.of(sourceBucket, sourcePath),
            Storage.BlobGetOption.userProject(targetProjectId));
        if (sourceBlob == null) {
            throw new PdaoSourceFileNotFoundException("Source file not found: '" + gspath + "'");
        }

        return sourceBlob;
    }

    private void fileAclOp(AclOp op, Dataset dataset, List<String> fileIds, Map<IamRole, String> policies)
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

        Map<String, GoogleBucketResource> bucketCache = new HashMap<>();

        int batchSize = configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE);
        List<List<String>> batches = ListUtils.partition(fileIds, batchSize);
        logger.info("operation {} on {} file ids, in {} batches of {}",
            op.name(), fileIds.size(), batches.size(), batchSize);

        int count = 0;
        for (List<String> batch : batches) {
            logger.info("operation {} batch {}", op.name(), count);
            count++;

            List<FSFile> files = fileDao.batchRetrieveById(dataset, batch, 0, true);
            for (FSFile file : files) {
                // Cache the bucket resources to avoid repeated database lookups
                GoogleBucketResource bucketForFile = bucketCache.get(file.getBucketResourceId());
                if (bucketForFile == null) {
                    bucketForFile = dataLocationService.lookupBucket(file.getBucketResourceId());
                    bucketCache.put(file.getBucketResourceId(), bucketForFile);
                }
                Storage storage = storageForBucket(bucketForFile);
                URI gsUri = URI.create(file.getGspath());
                String bucketPath = StringUtils.removeStart(gsUri.getPath(), "/");
                BlobId blobId = BlobId.of(bucketForFile.getName(), bucketPath);
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
            }
        }
    }
}
