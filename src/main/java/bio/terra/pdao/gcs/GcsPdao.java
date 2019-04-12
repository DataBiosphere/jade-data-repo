package bio.terra.pdao.gcs;

import bio.terra.metadata.FSObject;
import bio.terra.metadata.Study;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.exception.PdaoFileCopyException;
import bio.terra.pdao.exception.PdaoInvalidUriException;
import bio.terra.pdao.exception.PdaoSourceFileNotFoundException;
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

@Component
@Profile("google")
public class GcsPdao {
    private GcsConfiguration gcsConfiguration;
    private Storage storage;

    @Autowired
    public GcsPdao(GcsConfiguration gcsConfiguration, Storage storage) {
        this.gcsConfiguration = gcsConfiguration;
        this.storage = storage;
    }

    // We return the incoming FSObject with the blob information filled in
    public FSObject copyFile(Study study,
                         FileLoadModel fileLoadModel,
                         FSObject fsObject) {

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
            sourcePath = sourceUri.getPath();
        } catch (IllegalArgumentException ex) {
            throw new PdaoInvalidUriException("Invalid gs path: '" +
                fileLoadModel.getSourcePath() + "'", ex);
        }

        if (sourceBucket == null || sourcePath == null) {
            throw new PdaoInvalidUriException("Invalid gs path: '" +
                fileLoadModel.getSourcePath() + "'");
        }

        Blob sourceBlob = storage.get(BlobId.of(sourceBucket, sourcePath));
        if (sourceBlob == null) {
            throw new PdaoSourceFileNotFoundException("Source file not found: '" +
                fileLoadModel.getSourcePath() + "'");
        }

        // Our path is /<study-id>/<object-id>
        String targetPath = "/" + study.getId().toString() + "/" + fsObject.getObjectId();

        try {
            CopyWriter writer = sourceBlob.copyTo(BlobId.of(gcsConfiguration.getBucket(), targetPath));
            Blob targetBlob = writer.getResult();

            // Only supply the MD5 if it can be used to validate the entire contents of the file
            Integer componentCount = targetBlob.getComponentCount();
            String checksumMd5 = null;
            if (componentCount == null || componentCount == 1) {
                checksumMd5 = targetBlob.getMd5ToHexString();
            }

            fsObject
                .gspath("gs://" + gcsConfiguration.getBucket() + targetPath)
                .checksumMd5(checksumMd5)
                .checksumCrc32c(targetBlob.getCrc32cToHexString())
                .size(targetBlob.getSize());

        } catch (StorageException ex) {
            // For now, we assume that the storage exception is caused by bad input (the file copy exception
            // derives from BadRequestException). I think there are several cases here. We might need to retry
            // for flaky google case or we might need to bail out if access is denied.
            throw new PdaoFileCopyException("File ingest failed", ex);
        }

        return fsObject;
    }

}
