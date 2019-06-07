package bio.terra.service;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.FSEnumDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DirectoryDetailModel;
import bio.terra.model.DirectoryItemModel;
import bio.terra.model.FSObjectModel;
import bio.terra.model.FSObjectModelType;
import bio.terra.model.FileDetailModel;
import bio.terra.model.FileLoadModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
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

    private final Stairway stairway;
    private final FireStoreFileDao fileDao;

    @Autowired
    public FileService(Stairway stairway, FireStoreFileDao fileDao) {
        this.stairway = stairway;
        this.fileDao = fileDao;
    }

    public String deleteFile(String studyId, String fileId) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Delete file from study " + studyId + " file " + fileId);
        flightMap.put(JobMapKeys.STUDY_ID.getKeyName(), studyId);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), fileId);
        return stairway.submit(FileDeleteFlight.class, flightMap);
    }

    public String ingestFile(String studyId, FileLoadModel fileLoad) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Ingest file " + fileLoad.getTargetPath());
        flightMap.put(JobMapKeys.STUDY_ID.getKeyName(), studyId);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), fileLoad);
        return stairway.submit(FileIngestFlight.class, flightMap);
    }

    public FSObjectModel lookupFile(String studyId, String fileId) {
        return fileModelFromFSObject(lookupFSObject(studyId, fileId));
    }

    public FSObjectModel lookupPath(String studyId, String path) {
        return fileModelFromFSObject(lookupFSObjectByPath(studyId, path));
    }

    FSObjectBase lookupFSObject(String studyId, String fileId) {
        FSObjectBase fsObject = fileDao.retrieveEnum(UUID.fromString(studyId), UUID.fromString(fileId));
        checkFSObject(fsObject, studyId, fileId);
        return fsObject;
    }

    FSObjectBase lookupFSObjectByPath(String studyId, String path) {
        FSObjectBase fsObject = fileDao.retrieveEnumByPath(UUID.fromString(studyId), path);
        checkFSObject(fsObject, studyId, path);
        return fsObject;
    }

    private void checkFSObject(FSObjectBase fsObject, String studyId, String objectRef) {
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("File '" + objectRef + "' not found in study with id '"
                + studyId + "'");
        }

        switch (fsObject.getObjectType()) {
            case FILE:
            case DIRECTORY:
                break;

                // Don't reveal files that are coming or going
            case INGESTING_FILE:
            case DELETING_FILE:
            default:
                throw new FileSystemObjectNotFoundException("File '" + objectRef + "' not found in study with id '"
                    + studyId + "'");
        }
    }

    public FSObjectModel fileModelFromFSObject(FSObjectBase fsObject) {
        FSObjectModel fsObjectModel = new FSObjectModel()
            .objectId(fsObject.getObjectId().toString())
            .studyId(fsObject.getStudyId().toString())
            .path(fsObject.getPath())
            .size(fsObject.getSize())
            .created(fsObject.getCreatedDate().toString())
            .description(fsObject.getDescription());

        if (fsObject.getObjectType() == FSObjectType.FILE) {
            if (!(fsObject instanceof FSFile)) {
                throw new FileSystemCorruptException("Mismatched object type");
            }
            FSFile fsFile = (FSFile)fsObject;
            fsObjectModel.fileDetail(new FileDetailModel()
                .checksums(makeChecksums(fsFile))
                .accessUrl(fsFile.getGspath())
                .mimeType(fsFile.getMimeType()));
        } else {
            if (!(fsObject instanceof FSEnumDir)) {
                throw new FileSystemCorruptException("Object type/class mistake");
            }
            FSEnumDir fsEnumDir = (FSEnumDir)fsObject;
            DirectoryDetailModel directoryDetail = new DirectoryDetailModel();
            for (FSObjectBase fsItem : fsEnumDir.getContents()) {
                FSObjectModelType objectModelType;
                if (fsItem.getObjectType() == FSObjectType.FILE) {
                    objectModelType = FSObjectModelType.FILE;
                } else {
                    objectModelType = FSObjectModelType.DIRECTORY;
                }

                DirectoryItemModel directoryItem = new DirectoryItemModel()
                    .name(getObjectName(fsItem.getPath()))
                    .objectId(fsItem.getObjectId().toString())
                    .objectType(objectModelType);
                directoryDetail.addContentsItem(directoryItem);
            }
            fsObjectModel.directoryDetail(directoryDetail);
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
