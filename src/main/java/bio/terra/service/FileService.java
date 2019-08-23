package bio.terra.service;

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
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
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
    private final FireStoreDao fileDao;
    private  final DatasetService datasetService;

    @Autowired
    public FileService(Stairway stairway, FireStoreDao fileDao, DatasetService datasetService) {
        this.stairway = stairway;
        this.fileDao = fileDao;
        this.datasetService = datasetService;
    }

    public String deleteFile(String datasetId, String fileId) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Delete file from dataset " + datasetId + " file " + fileId);
        flightMap.put(JobMapKeys.DATASET_ID.getKeyName(), datasetId);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), fileId);
        return stairway.submit(FileDeleteFlight.class, flightMap);
    }

    public String ingestFile(String datasetId, FileLoadModel fileLoad) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Ingest file " + fileLoad.getTargetPath());
        flightMap.put(JobMapKeys.DATASET_ID.getKeyName(), datasetId);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), fileLoad);
        return stairway.submit(FileIngestFlight.class, flightMap);
    }

    public FSObjectModel lookupFile(String datasetId, String fileId) {
        return fileModelFromFSObject(lookupFSObject(datasetId, fileId));
    }

    public FSObjectModel lookupPath(String datasetId, String path) {
        FSObjectBase fsObject = lookupFSObjectByPath(datasetId, path);
        return fileModelFromFSObject(fsObject);
    }

    FSObjectBase lookupFSObject(String datasetId, String fileId) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveById(dataset, fileId, 1, true);
    }

    FSObjectBase lookupFSObjectByPath(String datasetId, String path) {
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
        return fileDao.retrieveById(dataset, path, 1, true);
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
