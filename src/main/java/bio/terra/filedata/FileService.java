package bio.terra.filedata;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dataset.DatasetService;
import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.filedata.exception.FileSystemCorruptException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSItem;
import bio.terra.metadata.Snapshot;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DirectoryDetailModel;
import bio.terra.model.FileDetailModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.FileModelType;
import bio.terra.service.JobMapKeys;
import bio.terra.service.JobService;
import bio.terra.snapshot.SnapshotService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger(FileService.class);

    private final JobService jobService;
    private final FireStoreDao fileDao;
    private final DatasetService datasetService;
    private final SnapshotService snapshotService;

    @Autowired
    public FileService(JobService jobService,
                       FireStoreDao fileDao,
                       DatasetService datasetService,
                       SnapshotService snapshotService) {
        this.fileDao = fileDao;
        this.datasetService = datasetService;
        this.jobService = jobService;
        this.snapshotService = snapshotService;
    }

    public String deleteFile(String datasetId, String fileId, AuthenticatedUserRequest userReq) {
        String description = "Delete file from dataset " + datasetId + " file " + fileId;
        return jobService
            .newJob(description, FileDeleteFlight.class, null, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
            .addParameter(JobMapKeys.FILE_ID.getKeyName(), fileId)
            .submit();
    }

    public String ingestFile(String datasetId, FileLoadModel fileLoad, AuthenticatedUserRequest userReq) {
        String description = "Ingest file " + fileLoad.getTargetPath();
        return jobService
            .newJob(description, FileIngestFlight.class, fileLoad, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
            .submit();
    }

    // -- dataset lookups --
    // depth == -1 means expand the entire sub-tree from this node
    // depth == 0 means no expansion - just this node
    // depth >= 1 means expand N levels
    public FileModel lookupFile(String datasetId, String fileId, int depth) {
        return fileModelFromFSItem(lookupFSItem(datasetId, fileId, depth));
    }

    public FileModel lookupPath(String datasetId, String path, int depth) {
        FSItem fsItem = lookupFSItemByPath(datasetId, path, depth);
        return fileModelFromFSItem(fsItem);
    }

    FSItem lookupFSItem(String datasetId, String fileId, int depth) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveById(dataset, fileId, depth, true);
    }

    FSItem lookupFSItemByPath(String datasetId, String path, int depth) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveByPath(dataset, path, depth, true);
    }

    // -- snapshot lookups --
    public FileModel lookupSnapshotFile(String snapshotId, String fileId, int depth) {
        return fileModelFromFSItem(lookupSnapshotFSItem(snapshotId, fileId, depth));
    }

    public FileModel lookupSnapshotPath(String snapshotId, String path, int depth) {
        FSItem fsItem = lookupSnapshotFSItemByPath(snapshotId, path, depth);
        return fileModelFromFSItem(fsItem);
    }

    FSItem lookupSnapshotFSItem(String snapshotId, String fileId, int depth) {
        Snapshot snapshot = snapshotService.retrieveSnapshot(UUID.fromString(snapshotId));
        return fileDao.retrieveById(snapshot, fileId, depth, true);
    }

    FSItem lookupSnapshotFSItemByPath(String snapshotId, String path, int depth) {
        Snapshot snapshot = snapshotService.retrieveSnapshot(UUID.fromString(snapshotId));
        return fileDao.retrieveByPath(snapshot, path, depth, true);
    }

    public FileModel fileModelFromFSItem(FSItem fsItem) {
        FileModel fileModel = new FileModel()
            .fileId(fsItem.getFileId().toString())
            .collectionId(fsItem.getCollectionId().toString())
            .path(fsItem.getPath())
            .size(fsItem.getSize())
            .created(fsItem.getCreatedDate().toString())
            .description(fsItem.getDescription())
            .checksums(makeChecksums(fsItem));

        if (fsItem instanceof FSFile) {
            fileModel.fileType(FileModelType.FILE);

            FSFile fsFile = (FSFile)fsItem;
            fileModel.fileDetail(new FileDetailModel()
                .datasetId(fsFile.getDatasetId().toString())
                .accessUrl(fsFile.getGspath())
                .mimeType(fsFile.getMimeType()));
        } else if (fsItem instanceof FSDir) {
            fileModel.fileType(FileModelType.DIRECTORY);
            FSDir fsDir = (FSDir)fsItem;
            DirectoryDetailModel directoryDetail = new DirectoryDetailModel().enumerated(fsDir.isEnumerated());
            if (fsDir.isEnumerated()) {
                directoryDetail.contents(new ArrayList<>());
                for (FSItem fsContentsItem : fsDir.getContents()) {
                    FileModel itemModel = fileModelFromFSItem(fsContentsItem);
                    directoryDetail.addContentsItem(itemModel);
                }
            }
            fileModel.directoryDetail(directoryDetail);
        } else {
            throw new FileSystemCorruptException("Entry type is totally wrong; we shouldn't be here");
        }

        return fileModel;
    }

    // We use the DRSChecksum model to represent the checksums in the repository
    // API's FileModel to return the set of checksums for a file.
    List<DRSChecksum> makeChecksums(FSItem fsItem) {
        List<DRSChecksum> checksums = new ArrayList<>();
        if (fsItem.getChecksumCrc32c() != null) {
            DRSChecksum checksumCrc32 = new DRSChecksum()
                .checksum(fsItem.getChecksumCrc32c())
                .type("crc32c");
            checksums.add(checksumCrc32);
        }

        if (fsItem.getChecksumMd5() != null) {
            DRSChecksum checksumMd5 = new DRSChecksum()
                .checksum(fsItem.getChecksumMd5())
                .type("md5");
            checksums.add(checksumMd5);
        }

        return checksums;
    }
}
