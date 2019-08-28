package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DatasetDao;
import bio.terra.dao.SnapshotDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.dao.exception.SnapshotNotFoundException;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.exception.NotImplementedException;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
    private final FireStoreFileDao fileDao;
    private final FileService fileService;
    private final DrsIdService drsIdService;
    private final GcsConfiguration gcsConfiguration;
    private final DatasetService datasetService;
    private final SamClientService samService;

    @Autowired
    public DrsService(DatasetDao datasetDao,
                      SnapshotDao snapshotDao,
                      FireStoreFileDao fileDao,
                      FileService fileService,
                      DrsIdService drsIdService,
                      GcsConfiguration gcsConfiguration,
                      DatasetService datasetService,
                      SamClientService samService) {
        this.datasetDao = datasetDao;
        this.snapshotDao = snapshotDao;
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.drsIdService = drsIdService;
        this.gcsConfiguration = gcsConfiguration;
        this.datasetService = datasetService;
        this.samService = samService;
    }

    public DRSObject lookupObjectByDrsId(AuthenticatedUserRequest authUser, String drsObjectId, Boolean expand) {
        DrsId drsId = parseAndValidateDrsId(drsObjectId);
        // Make sure requester is a READER on the snapshot
        samService.verifyAuthorization(
            authUser,
            SamClientService.ResourceType.DATASNAPSHOT,
            drsId.getSnapshotId(),
            SamClientService.DataRepoAction.READ_DATA);

        // TODO: Implement recursive directory expansion
        if (expand) {
            throw new NotImplementedException("Expand is not yet implemented");
        }
        FSObjectBase fsObject = fileService.lookupFSObject(
            drsId.getDatasetId(),
            drsId.getFsObjectId());

        switch (fsObject.getObjectType()) {
            case FILE:
                return drsObjectFromFSFile((FSFile)fsObject, drsId.getSnapshotId(), authUser);

            case DIRECTORY:
                return drsObjectFromFSDir((FSDir)fsObject, drsId.getSnapshotId());

            default:
                throw new IllegalArgumentException("Invalid object type");
        }
    }

    private DRSObject drsObjectFromFSFile(FSFile fsFile, String snapshotId, AuthenticatedUserRequest authUser) {
        DRSObject fileObject = makeCommonDrsObject(fsFile, snapshotId);

        DRSAccessURL gsAccessURL = new DRSAccessURL()
            .url(fsFile.getGspath());

        DRSAccessMethod gsAccessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .region(fsFile.getRegion());

        DRSAccessURL httpsAccessURL = new DRSAccessURL()
            .url(makeHttpsFromGs(fsFile.getGspath()))
            .headers(makeAuthHeader(authUser));

        DRSAccessMethod httpsAccessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(fsFile.getRegion());

        List<DRSAccessMethod> accessMethods = new ArrayList<>();
        accessMethods.add(gsAccessMethod);
        accessMethods.add(httpsAccessMethod);

        fileObject
            .size(fsFile.getSize())
            .mimeType(fsFile.getMimeType())
            .checksums(fileService.makeChecksums(fsFile))
            .accessMethods(accessMethods);

        return fileObject;
    }

    private DRSObject drsObjectFromFSDir(FSDir fsDir, String snapshotId) {
        DRSObject dirObject = makeCommonDrsObject(fsDir, snapshotId);

        // TODO: Directory size and checksum not yet implemented
        DRSChecksum drsChecksum = new DRSChecksum().type("crc32c").checksum("0");
        dirObject
            .size(0L)
            .addChecksumsItem(drsChecksum)
            .contents(makeContentsList(fsDir, snapshotId));

        return dirObject;
    }

    private DRSObject makeCommonDrsObject(FSObjectBase fsObject, String snapshotId) {
        // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
        String theTime = fsObject.getCreatedDate().toString();
        DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

        return new DRSObject()
            .id(drsId.toDrsObjectId())
            .name(getLastNameFromPath(fsObject.getPath()))
            .selfUri(drsId.toDrsUri())
            .createdTime(theTime)
            .updatedTime(theTime)
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

    private DRSContentsObject makeDrsContentsObject(FSObjectBase fsObject, String snapshotId) {
        DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

        List<String> drsUris = new ArrayList<>();
        drsUris.add(drsId.toDrsUri());

        DRSContentsObject contentsObject = new DRSContentsObject()
            .name(getLastNameFromPath(fsObject.getPath()))
            .id(drsId.toDrsObjectId())
            .drsUri(drsUris);

        // If the object is an enumerated directory, we fill in the contents array.
        if (fsObject.getObjectType() == FSObjectType.DIRECTORY) {
            FSDir fsDir = (FSDir) fsObject;
            if (fsDir.isEnumerated()) {
                contentsObject.contents(makeContentsList(fsDir, snapshotId));
            }
        }

        return contentsObject;
    }

    private String makeHttpsFromGs(String gspath) {
        try {
            URI gsuri = URI.create(gspath);
            String gsBucket = gsuri.getAuthority();
            String gsPath = StringUtils.removeStart(gsuri.getPath(), "/");
            String encodedPath = URLEncoder.encode(gsPath, StandardCharsets.UTF_8.toString());
            return String.format("https://www.googleapis.com/storage/v1/b/%s/o/%s?alt=media", gsBucket, encodedPath);
        } catch (UnsupportedEncodingException ex) {
            throw new InternalServerErrorException("Failed to urlencode file path", ex);
        }
    }

    private List<String> makeAuthHeader(AuthenticatedUserRequest authUser) {
        String hdr = String.format("Authorization: Bearer %s", authUser.getRequiredToken());
        return Collections.singletonList(hdr);
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
