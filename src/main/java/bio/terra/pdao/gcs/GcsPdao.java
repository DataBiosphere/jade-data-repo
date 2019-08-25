package bio.terra.pdao.gcs;

import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.FireStoreFile;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.exception.PdaoException;
import bio.terra.pdao.exception.PdaoFileCopyException;
import bio.terra.pdao.exception.PdaoInvalidUriException;
import bio.terra.pdao.exception.PdaoSourceFileNotFoundException;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.service.dataproject.DataLocationService;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
@Profile("google")
public class GcsPdao {
    private static final Logger logger = LoggerFactory.getLogger(GcsPdao.class);
    private static final int DATASET_DELETE_BATCH_SIZE = 1000;

    private final GcsProjectFactory gcsProjectFactory;
    private final DataLocationService dataLocationService;
    private final FireStoreDao fileDao;

    @Autowired
    public GcsPdao(
        GcsProjectFactory gcsProjectFactory,
        DataLocationService dataLocationService,
        FireStoreDao fileDao) {
        this.gcsProjectFactory = gcsProjectFactory;
        this.dataLocationService = dataLocationService;
        this.fileDao = fileDao;
    }

    private Storage storageForBucket(GoogleBucketResource bucketResource) {
        GoogleProjectResource projectResource = bucketResource.getProjectResource();
        GcsProject gcsProject = gcsProjectFactory.get(projectResource.getGoogleProjectId());
        return gcsProject.getStorage();
    }

    // We return the incoming FSObject with the blob information filled in
    public FSFileInfo copyFile(Dataset dataset,
                               FileLoadModel fileLoadModel,
                               String objectId,
                               GoogleBucketResource bucketResource) {
        String sourceBucket;
        String sourcePath;

        try {
            URI sourceUri = URI.create(fileLoadModel.getSourcePath());
            if (!StringUtils.equals(sourceUri.getScheme(), "gs")) {
                throw new PdaoInvalidUriException("Source path is not a gs path: '" +
                    fileLoadModel.getSourcePath() + "'");
            }
            if (sourceUri.getPort() != -1) {
                throw new PdaoInvalidUriException("Source path must not have a port specified: '" +
                    fileLoadModel.getSourcePath() + "'");
            }
            sourceBucket = sourceUri.getAuthority();
            sourcePath = StringUtils.removeStart(sourceUri.getPath(), "/");
        } catch (IllegalArgumentException ex) {
            throw new PdaoInvalidUriException("Invalid gs path: '" +
                fileLoadModel.getSourcePath() + "'", ex);
        }

        if (sourceBucket == null || sourcePath == null) {
            throw new PdaoInvalidUriException("Invalid gs path: '" +
                fileLoadModel.getSourcePath() + "'");
        }

        Storage storage = storageForBucket(bucketResource);
        Blob sourceBlob = storage.get(BlobId.of(sourceBucket, sourcePath));
        if (sourceBlob == null) {
            throw new PdaoSourceFileNotFoundException("Source file not found: '" +
                fileLoadModel.getSourcePath() + "'");
        }

        // Our path is /<dataset-id>/<object-id>
        String targetPath = dataset.getId().toString() + "/" + objectId;

        try {
            // The documentation is vague whether or not it is important to copy by chunk. One set of
            // examples does it and another doesn't.
            //
            // I have been seeing timeouts and I think they are due to particularly large files,
            // so I changed exported the timeouts to application.properties to allow for tuning
            // and I am changing this to copy chunks.
            CopyWriter writer = sourceBlob.copyTo(BlobId.of(bucketResource.getName(), targetPath));
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
                .objectId(objectId)
                .createdDate(createTime.toString())
                .gspath(gspath.toString())
                .checksumCrc32c(targetBlob.getCrc32cToHexString())
                .checksumMd5(checksumMd5)
                .size(targetBlob.getSize())
                .region(bucketResource.getRegion())
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

    public boolean deleteFile(String inGspath, GoogleBucketResource bucketResource) {
        GcsProject gcsProject = gcsProjectFactory.get(bucketResource.getProjectResource().getGoogleProjectId());
        Storage storage = gcsProject.getStorage();

        // It's possible that the file didn't get written to a bucket, meaning gspath will be null.
        Optional<String> gspath = Optional.ofNullable(inGspath);
        if (gspath.isPresent()) {
            URI uri = URI.create(gspath.get());
            String bucketPath = StringUtils.removeStart(uri.getPath(), "/");
            Optional<Blob> blob = Optional.ofNullable(storage.get(BlobId.of(bucketResource.getName(), bucketPath)));
            if (blob.isPresent()) {
                return blob.get().delete();
            }
        }
        return false;
    }

    // Consumer method for deleting GCS files driven from a scan over the firestore files
    public void deleteFile(FireStoreFile fireStoreFile) {
        if (fireStoreFile != null) {
            GoogleBucketResource bucketResource =
                dataLocationService.getBucketForFile(fireStoreFile.getProfileId(), fireStoreFile.getBucketResourceId());
            deleteFile(fireStoreFile.getGspath(), bucketResource);
        }
    }

    private enum AclOp {
        ACL_OP_CREATE,
        ACL_OP_DELETE
    };

    public void setAclOnFiles(Dataset dataset, List<String> fileIds, String readersPolicyEmail) {
        fileAclOp(AclOp.ACL_OP_CREATE, dataset, fileIds, readersPolicyEmail);
    }

    public void removeAclOnFiles(Dataset dataset, List<String> fileIds, String readersPolicyEmail) {
        fileAclOp(AclOp.ACL_OP_DELETE, dataset, fileIds, readersPolicyEmail);
    }

    private void fileAclOp(AclOp op, Dataset dataset, List<String> fileIds, String readersPolicyEmail) {
        Acl.Group readerGroup = new Acl.Group(readersPolicyEmail);
        Acl acl = Acl.newBuilder(readerGroup, Acl.Role.READER).build();

        for (String fileId : fileIds) {
            FSObjectBase fsObjectBase = fileDao.retrieveById(dataset, fileId, 0, true);
            if (fsObjectBase instanceof FSFile) {
                FSFile fsFile = (FSFile)fsObjectBase;
                GoogleBucketResource bucketForFile =
                    dataLocationService.getBucketForFile(fsFile.getProfileId(), fsFile.getBucketResourceId());
                Storage storage = storageForBucket(bucketForFile);
                URI gsUri = URI.create(fsFile.getGspath());
                String bucketPath = StringUtils.removeStart(gsUri.getPath(), "/");
                BlobId blobId = BlobId.of(bucketForFile.getName(), bucketPath);
                switch (op) {
                    case ACL_OP_CREATE:
                        storage.createAcl(blobId, acl);
                        break;
                    case ACL_OP_DELETE:
                        storage.deleteAcl(blobId, readerGroup);
                        break;
                }
            }
        }
    }
}
