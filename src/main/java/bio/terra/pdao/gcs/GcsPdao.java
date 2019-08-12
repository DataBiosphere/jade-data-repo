package bio.terra.pdao.gcs;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.FileDataBucket;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.exception.PdaoException;
import bio.terra.pdao.exception.PdaoFileCopyException;
import bio.terra.pdao.exception.PdaoInvalidUriException;
import bio.terra.pdao.exception.PdaoSourceFileNotFoundException;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.service.dataproject.DataProjectService;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
@Profile("google")
public class GcsPdao {
    private final GcsProjectFactory gcsProjectFactory;
    private final DataProjectService dataProjectService;
    private final FireStoreFileDao fileDao;

    @Autowired
    public GcsPdao(
        GcsProjectFactory gcsProjectFactory,
        DataProjectService dataProjectService,
        FireStoreFileDao fileDao) {
        this.gcsProjectFactory = gcsProjectFactory;
        this.dataProjectService = dataProjectService;
        this.fileDao = fileDao;
    }

    private Storage storageForBucket(FileDataBucket fileDataBucket) {
        GoogleProjectResource projectResource = fileDataBucket.getProjectResource()
            .orElseThrow(IllegalStateException::new);
        GcsProject gcsProject = gcsProjectFactory.get(projectResource.getGoogleProjectId());
        return gcsProject.getStorage();
    }

    // We return the incoming FSObject with the blob information filled in
    public FSFileInfo copyFile(Dataset dataset,
                               FileLoadModel fileLoadModel,
                               FSFile fsObject) {
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

        // Figure out where the destination bucket is. Similar to how there is a ProjectIdSelector there will be need
        // to be a BucketSelector that picks a (Project, Bucket) pair for the file. The Resource Manager will then need
        // to potentially create the project and/or the bucket inside of that project depending on what already exists.
        FileDataBucket bucketForFile = dataProjectService.getBucketForFile(fsObject);
        Storage storage = storageForBucket(bucketForFile);

        Blob sourceBlob = storage.get(BlobId.of(sourceBucket, sourcePath));
        if (sourceBlob == null) {
            throw new PdaoSourceFileNotFoundException("Source file not found: '" +
                fileLoadModel.getSourcePath() + "'");
        }

        // Our path is /<dataset-id>/<object-id>
        String targetPath = dataset.getId().toString() + "/" + fsObject.getObjectId();

        try {
            // The documentation is vague whether or not it is important to copy by chunk. One set of
            // examples does it and another doesn't.
            //
            // I have been seeing timeouts and I think they are due to particularly large files,
            // so I changed exported the timeouts to application.properties to allow for tuning
            // and I am changing this to copy chunks.
            CopyWriter writer = sourceBlob.copyTo(BlobId.of(bucketForFile.getName(), targetPath));
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
                bucketForFile.getName(),
                "/" + targetPath,
                null,
                null);

            FSFileInfo fsFileInfo = new FSFileInfo()
                .objectId(fsObject.getObjectId().toString())
                .datasetId(fsObject.getDatasetId().toString())
                .createdDate(createTime.toString())
                .gspath(gspath.toString())
                .checksumCrc32c(targetBlob.getCrc32cToHexString())
                .checksumMd5(checksumMd5)
                .size(targetBlob.getSize());

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

    public boolean deleteFile(FSFile fsFile) {
        FileDataBucket bucketForFile = dataProjectService.getBucketForFile(fsFile);
        Storage storage = storageForBucket(bucketForFile);
        URI uri = URI.create(fsFile.getGspath());
        String bucketPath = StringUtils.removeStart(uri.getPath(), "/");
        Blob blob = storage.get(BlobId.of(bucketForFile.getName(), bucketPath));
        if (blob == null) {
            return false;
        }
        return blob.delete();
    }

    public void deleteFilesFromDataset(Dataset dataset) {
        // these files could be in multiple buckets..need to enumerate them from firestore and then iterate + delete
        fileDao.forEachFsObjectInDataset(dataset, fsObjectBase -> {
            if (fsObjectBase.getObjectType() != FSObjectType.DIRECTORY) {
                deleteFile((FSFile)fsObjectBase);
            }
        });
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
            FSObjectBase fsObjectBase = fileDao.retrieve(dataset, UUID.fromString(fileId));
            if (fsObjectBase.getObjectType() == FSObjectType.FILE) {
                FSFile fsFile = (FSFile)fsObjectBase;
                FileDataBucket fileDataBucket = dataProjectService.getBucketForFile(fsFile);
                Storage storage = storageForBucket(fileDataBucket);
                URI gsUri = URI.create(fsFile.getGspath());
                String bucketPath = StringUtils.removeStart(gsUri.getPath(), "/");
                BlobId blobId = BlobId.of(fileDataBucket.getName(), bucketPath);
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
