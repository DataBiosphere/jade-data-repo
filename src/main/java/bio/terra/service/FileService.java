package bio.terra.service;

import bio.terra.controller.UserInfo;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.*;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DirectoryDetailModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FSObjectModelType;
import bio.terra.model.FileDetailModel;
import bio.terra.model.FileLoadModel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.FileService");

    private final JobService jobService;
    private final FireStoreFileDao fileDao;
    private  final DatasetService datasetService;

    @Autowired
    public FileService(JobService jobService, FireStoreFileDao fileDao, DatasetService datasetService) {
        this.jobService = jobService;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
    }

    public String deleteFile(String datasetId, String fileId, UserInfo userInfo) {
        return jobService.submit(
            "Delete file from dataset " + datasetId + " file " + fileId,
            FileDeleteFlight.class,
            fileId,
            userInfo);
    }

    public String ingestFile(String datasetId, FileLoadModel fileLoad, UserInfo userInfo) {
        return jobService.submit(
            "Ingest file " + fileLoad.getTargetPath(),
            FileIngestFlight.class,
            fileLoad,
            userInfo);
    }

    public FSObjectModel lookupFile(String datasetId, String fileId) {
        return fileModelFromFSObject(lookupFSObject(datasetId, fileId));
    }

    public FSObjectModel lookupPath(String datasetId, String path) {
        FSObjectBase fsObject = lookupFSObjectByPath(datasetId, path);
        return fileModelFromFSObject(fsObject);
    }

    public FSObjectBase lookupFSObject(String datasetId, String fileId) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        FSObjectBase fsObject = fileDao.retrieveWithContents(dataset, UUID.fromString(fileId));
        checkFSObject(fsObject, datasetId, fileId);
        return fsObject;
    }

    private FSObjectBase lookupFSObjectByPath(String datasetId, String path) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        FSObjectBase fsObject = fileDao.retrieveWithContentsByPath(dataset, path);
        checkFSObject(fsObject, datasetId, path);
        return fsObject;
    }

    private void checkFSObject(FSObjectBase fsObject, String datasetId, String objectRef) {
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File '" + objectRef + "' not found in dataset with id '"
                + datasetId + "'");
        }

        switch (fsObject.getObjectType()) {
            case FILE:
            case DIRECTORY:
                break;

                // Don't reveal files that are coming or going
            case INGESTING_FILE:
            case DELETING_FILE:
            default:
                throw new FileSystemObjectNotFoundException("File '" + objectRef + "' not found in dataset with id '"
                    + datasetId + "'");
        }
    }

    public FSObjectModel fileModelFromFSObject(FSObjectBase fsObject) {
        FSObjectModel fsObjectModel = new FSObjectModel()
            .objectId(fsObject.getObjectId().toString())
            .datasetId(fsObject.getDatasetId().toString())
            .path(fsObject.getPath())
            .size(fsObject.getSize())
            .created(fsObject.getCreatedDate().toString())
            .description(fsObject.getDescription());

        if (fsObject.getObjectType() == FSObjectType.FILE) {
            if (!(fsObject instanceof FSFile)) {
                throw new FileSystemCorruptException("Mismatched object type");
            }
            fsObjectModel.objectType(FSObjectModelType.FILE);

            FSFile fsFile = (FSFile)fsObject;
            fsObjectModel.fileDetail(new FileDetailModel()
                .checksums(makeChecksums(fsFile))
                .accessUrl(fsFile.getGspath())
                .mimeType(fsFile.getMimeType()));
        } else if (fsObject.getObjectType() == FSObjectType.DIRECTORY) {
            if (!(fsObject instanceof FSDir)) {
                throw new FileSystemCorruptException("Mismatched object type");
            }

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

    // Even though this uses the DRSChecksum model, it is used in the
    // FileModel to return the set of checksums for a file.
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

    private String getObjectName(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

}
