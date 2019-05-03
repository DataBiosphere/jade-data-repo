package bio.terra.service;

import bio.terra.filesystem.FileDao;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.FSObject;
import bio.terra.model.DRSChecksum;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
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

import static bio.terra.metadata.FSObject.FSObjectType.DIRECTORY;
import static bio.terra.metadata.FSObject.FSObjectType.FILE;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.FileService");

    private final Stairway stairway;
    private final FileDao fileDao;

    @Autowired
    public FileService(Stairway stairway, FileDao fileDao) {
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

    public FileModel lookupFile(String studyId, String fileId) {
        return fileModelFromFSObject(lookupFSObject(studyId, fileId, FSObject.FSObjectType.FILE));
    }

    FSObject lookupFSObject(String studyId, String fileId, FSObject.FSObjectType objectType) {
        FSObject fsObject = fileDao.retrieve(UUID.fromString(fileId));

        if (!StringUtils.equals(fsObject.getStudyId().toString(), studyId)) {
            throw new FileSystemObjectNotFoundException("File with id '" + fileId + "' not found in study with id '"
                + studyId + "'");
        }

        switch (fsObject.getObjectType()) {
            case FILE:
                if (objectType == FILE) {
                    return fsObject;
                } else {
                    throw new InvalidFileSystemObjectTypeException("Attempt to lookup a file");
                }

            case DIRECTORY:
                if (objectType == DIRECTORY) {
                    return fsObject;
                } else {
                    throw new InvalidFileSystemObjectTypeException("Attempt to lookup a directory");
                }

            // Don't reveal files that are coming or going
            case INGESTING_FILE:
            case DELETING_FILE:
            default:
                throw new FileSystemObjectNotFoundException("File with id '" + fileId + "' not found in study with id '"
                    + studyId + "'");
        }
    }

    public FileModel fileModelFromFSObject(FSObject fsObject) {
        FileModel fileModel = new FileModel()
            .fileId(fsObject.getObjectId().toString())
            .studyId(fsObject.getStudyId().toString())
            .path(fsObject.getPath())
            .size(fsObject.getSize())
            .created(fsObject.getCreatedDate().toString())
            .mimeType(fsObject.getMimeType())
            .checksums(makeChecksums(fsObject))
            .accessUrl(fsObject.getGspath())
            .description(fsObject.getDescription());

        return fileModel;
    }

    // Even though this uses the DRSChecksum model, it is used in the
    // FileModel to return the set of checksums for a file.
    List<DRSChecksum> makeChecksums(FSObject fsObject) {
        List<DRSChecksum> checksums = new ArrayList<>();
        DRSChecksum checksumCrc32 = new DRSChecksum()
            .checksum(fsObject.getChecksumCrc32c())
            .type("crc32c");
        checksums.add(checksumCrc32);

        if (fsObject.getChecksumMd5() != null) {
            DRSChecksum checksumMd5 = new DRSChecksum()
                .checksum(fsObject.getChecksumMd5())
                .type("md5");
            checksums.add(checksumMd5);
        }

        return checksums;
    }
}
