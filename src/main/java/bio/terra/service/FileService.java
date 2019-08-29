package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.filesystem.FireStoreDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DirectoryDetailModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FSObjectModelType;
import bio.terra.model.FileDetailModel;
import bio.terra.model.FileLoadModel;
import bio.terra.stairway.Stairway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.FileService");

    private final JobService jobService;
    private final Stairway stairway;
    private final FireStoreDao fileDao;
    private  final DatasetService datasetService;

    @Autowired
    public FileService(JobService jobService, Stairway stairway, FireStoreDao fileDao, DatasetService datasetService) {
        this.stairway = stairway;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
        this.jobService = jobService;
    }

    public String deleteFile(String datasetId, String fileId, AuthenticatedUserRequest userReq) {
        return jobService.submit(
            "Delete file from dataset " + datasetId + " file " + fileId,
            FileDeleteFlight.class,
            null,
            Stream.of(
                new AbstractMap.SimpleImmutableEntry<>(JobMapKeys.DATASET_ID.getKeyName(), datasetId),
                new AbstractMap.SimpleImmutableEntry<>(JobMapKeys.FILE_ID.getKeyName(), fileId))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            userReq);
    }

    public String ingestFile(String datasetId, FileLoadModel fileLoad, AuthenticatedUserRequest userReq) {
        return jobService.submit(
            "Ingest file " + fileLoad.getTargetPath(),
            FileIngestFlight.class,
            fileLoad,
            Collections.singletonMap(JobMapKeys.DATASET_ID.getKeyName(), datasetId),
            userReq);
    }

    // depth == -1 means expand the entire sub-tree from this node
    // depth == 0 means no expansion - just this node
    // depth >= 1 means expand N levels
    public FSObjectModel lookupFile(String datasetId, String fileId, int depth) {
        return fileModelFromFSObject(lookupFSObject(datasetId, fileId, depth));
    }

    public FSObjectModel lookupPath(String datasetId, String path, int depth) {
        FSObjectBase fsObject = lookupFSObjectByPath(datasetId, path, depth);
        return fileModelFromFSObject(fsObject);
    }

    public FSObjectBase lookupFSObject(String datasetId, String fileId, int depth) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveById(dataset, fileId, depth, true);
    }

    public FSObjectBase lookupFSObjectByPath(String datasetId, String path, int depth) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveByPath(dataset, path, depth, true);
    }

    public FSObjectModel fileModelFromFSObject(FSObjectBase fsObject) {
        FSObjectModel fsObjectModel = new FSObjectModel()
            .objectId(fsObject.getObjectId().toString())
            .datasetId(fsObject.getDatasetId().toString())
            .path(fsObject.getPath())
            .size(fsObject.getSize())
            .created(fsObject.getCreatedDate().toString())
            .description(fsObject.getDescription());

        if (fsObject instanceof FSFile) {
            fsObjectModel.objectType(FSObjectModelType.FILE);

            FSFile fsFile = (FSFile)fsObject;
            fsObjectModel.fileDetail(new FileDetailModel()
                .checksums(makeChecksums(fsFile))
                .accessUrl(fsFile.getGspath())
                .mimeType(fsFile.getMimeType()));
        } else if (fsObject instanceof FSDir) {
            fsObjectModel.objectType(FSObjectModelType.DIRECTORY);
            FSDir fsDir = (FSDir)fsObject;
            if (fsDir.isEnumerated()) {
                DirectoryDetailModel directoryDetail = new DirectoryDetailModel().contents(new ArrayList<>());
                for (FSObjectBase fsItem : fsDir.getContents()) {
                    FSObjectModel itemModel = fileModelFromFSObject(fsItem);
                    directoryDetail.addContentsItem(itemModel);
                }
                fsObjectModel.directoryDetail(directoryDetail);
            }
        } else {
            throw new FileSystemCorruptException("Object type instance is totally wrong; we shouldn't be here");
        }

        return fsObjectModel;
    }

    // We use the DRSChecksum model to represent the checksums in the repository
    // API's FileModel to return the set of checksums for a file.
    List<DRSChecksum> makeChecksums(FSFile fsFile) {
        List<DRSChecksum> checksums = new ArrayList<>();
        DRSChecksum checksumCrc32 = new DRSChecksum()
            .checksum(fsFile.getChecksumCrc32c())
            .type("crc32c");
        checksums.add(checksumCrc32);

        if (fsFile.getChecksumMd5() != null) {
            DRSChecksum checksumMd5 = new DRSChecksum()
                .checksum(fsFile.getChecksumMd5())
                .type("md5");
            checksums.add(checksumMd5);
        }

        return checksums;
    }
}
