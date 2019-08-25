package bio.terra.service;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.SnapshotDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.dao.exception.SnapshotNotFoundException;
import bio.terra.filesystem.FireStoreDirectoryDao;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.pdao.gcs.GcsConfiguration;
import bio.terra.service.exception.DrsObjectNotFoundException;
import bio.terra.service.exception.InvalidDrsIdException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Component
public class DrsService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.DrsService");

    private static final String DRS_OBJECT_VERSION = "0";

    private final DatasetDao datasetDao;
    private final SnapshotDao snapshotDao;
    private final FireStoreDirectoryDao fileDao;
    private final FileService fileService;
    private final DrsIdService drsIdService;
    private final GcsConfiguration gcsConfiguration;
    private final DatasetService datasetService;

    @Autowired
    public DrsService(DatasetDao datasetDao,
                      SnapshotDao snapshotDao,
                      FireStoreDirectoryDao fileDao,
                      FileService fileService,
                      DrsIdService drsIdService,
                      GcsConfiguration gcsConfiguration,
                      DatasetService datasetService) {
        this.datasetDao = datasetDao;
        this.snapshotDao = snapshotDao;
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.drsIdService = drsIdService;
        this.gcsConfiguration = gcsConfiguration;
        this.datasetService = datasetService;
    }

    public DRSObject lookupObjectByDrsId(String drsObjectId, Boolean expand) {
        DrsId drsId = parseAndValidateDrsId(drsObjectId);
        int depth = (expand ? -1 : 1);

        FSObjectBase fsObject = fileService.lookupFSObject(
            drsId.getDatasetId(),
            drsId.getFsObjectId(),
            depth);

        if (fsObject instanceof FSFile) {
            return drsObjectFromFSFile((FSFile)fsObject, fsObject.getDatasetId().toString());
        } else if (fsObject instanceof FSDir) {
            return drsObjectFromFSDir((FSDir)fsObject, fsObject.getDatasetId().toString());
        }

        throw new IllegalArgumentException("Invalid object type");
    }

    private DRSObject drsObjectFromFSFile(FSFile fsFile, String datasetId) {
        DRSObject fileObject = makeCommonDrsObject(fsFile, datasetId);

        DRSAccessURL accessURL = new DRSAccessURL()
            .url(fsFile.getGspath());

        DRSAccessMethod accessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(accessURL)
            .region(fsFile.getRegion());

        fileObject
            .size(fsFile.getSize())
            .mimeType(fsFile.getMimeType())
            .checksums(fileService.makeChecksums(fsFile))
            .accessMethods(Collections.singletonList(accessMethod));

        return fileObject;
    }

    private DRSObject drsObjectFromFSDir(FSDir fsDir, String datasetId) {
        DRSObject dirObject = makeCommonDrsObject(fsDir, datasetId);

        // TODO: Directory size and checksum not yet implemented
        DRSChecksum drsChecksum = new DRSChecksum().type("crc32c").checksum("0");
        dirObject
            .size(0L)
            .addChecksumsItem(drsChecksum)
            .contents(makeContentsList(fsDir, datasetId));

        return dirObject;
    }

    private DRSObject makeCommonDrsObject(FSObjectBase fsObject, String datasetId) {
        // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
        String theTime = fsObject.getCreatedDate().toString();
        DrsId drsId = makeDrsId(fsObject, datasetId);

        return new DRSObject()
            .id(drsId.toDrsObjectId())
            .name(getLastNameFromPath(fsObject.getPath()))
            .created(theTime)
            .updated(theTime)
            .version(DRS_OBJECT_VERSION)
            .description(fsObject.getDescription())
            .aliases(Collections.singletonList(fsObject.getPath()));
    }

    private List<DRSContentsObject> makeContentsList(FSDir fsDir, String datasetId) {
        List<DRSContentsObject> contentsList = new ArrayList<>();

        for (FSObjectBase fsObject : fsDir.getContents()) {
            contentsList.add(makeDrsContentsObject(fsObject, datasetId));
        }

        return contentsList;
    }

    private DRSContentsObject makeDrsContentsObject(FSObjectBase fsObject, String datasetId) {
        DrsId drsId = makeDrsId(fsObject, datasetId);

        List<String> drsUris = new ArrayList<>();
        drsUris.add(drsId.toDrsUri());

        DRSContentsObject contentsObject = new DRSContentsObject()
            .name(getLastNameFromPath(fsObject.getPath()))
            .id(drsId.toDrsObjectId())
            .drsUri(drsUris);

        if (fsObject instanceof FSDir) {
            FSDir fsDir = (FSDir) fsObject;
            if (fsDir.isEnumerated()) {
                contentsObject.contents(makeContentsList(fsDir, datasetId));
            }
        }

        return contentsObject;
    }

    private DrsId makeDrsId(FSObjectBase fsObject, String snapshotId) {
        return DrsId.builder()
            .datasetId(fsObject.getDatasetId().toString())
            .snapshotId(snapshotId)
            .fsObjectId(fsObject.getObjectId().toString())
            .build();
    }

    private String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

    private DrsId parseAndValidateDrsId(String drsObjectId) {
        DrsId drsId = drsIdService.fromObjectId(drsObjectId);
        try {
            UUID datasetId = UUID.fromString(drsId.getDatasetId());
            datasetDao.retrieveSummaryById(datasetId);

            UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
            snapshotDao.retrieveSnapshotSummary(snapshotId);

            return drsId;
        } catch (IllegalArgumentException ex) {
            throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
        } catch (DatasetNotFoundException ex) {
            throw new DrsObjectNotFoundException("No dataset found for DRS object id '" + drsObjectId + "'", ex);
        } catch (SnapshotNotFoundException ex) {
            throw new DrsObjectNotFoundException("No snapshot found for DRS object id '" + drsObjectId + "'", ex);
        }
    }
}
