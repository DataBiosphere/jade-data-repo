package bio.terra.service;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.filesystem.FileDao;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.FSObject;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSBundle;
import bio.terra.model.DRSBundleObject;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.pdao.gcs.GcsConfiguration;
import bio.terra.service.exception.DrsObjectNotFoundException;
import bio.terra.service.exception.InvalidDrsIdException;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static bio.terra.metadata.FSObject.FSObjectType.DIRECTORY;
import static bio.terra.metadata.FSObject.FSObjectType.FILE;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.FileService");

    private static final String DRS_OBJECT_VERSION = "0";

    private final Stairway stairway;
    private final StudyDao studyDao;
    private final DatasetDao datasetDao;
    private final FileDao fileDao;
    private final DrsIdService drsIdService;
    private final GcsConfiguration gcsConfiguration;

    @Autowired
    public FileService(Stairway stairway,
                       StudyDao studyDao,
                       DatasetDao datasetDao,
                       FileDao fileDao,
                       DrsIdService drsIdService,
                       GcsConfiguration gcsConfiguration) {
        this.stairway = stairway;
        this.studyDao = studyDao;
        this.datasetDao = datasetDao;
        this.fileDao = fileDao;
        this.drsIdService = drsIdService;
        this.gcsConfiguration = gcsConfiguration;
    }

    // -- file API support --

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

    // -- DRS API support --

    public DRSObject lookupObjectByDrsId(String drsObjectId) {
        DrsId drsId = drsIdService.fromObjectId(drsObjectId);

        try {
            UUID studyId = UUID.fromString(drsId.getStudyId());
            studyDao.retrieveSummaryById(studyId);

            UUID datasetId = UUID.fromString(drsId.getDatasetId());
            datasetDao.retrieveDatasetSummary(datasetId);

            // TODO: validate dataset and check permissions.

            FSObject fsObject = lookupFSObject(drsId.getStudyId(), drsId.getFsObjectId(), FSObject.FSObjectType.FILE);
            return drsObjectFromFSObject(fsObject, drsId.getDatasetId());

        } catch (IllegalArgumentException ex) {
            throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
        } catch (StudyNotFoundException | DatasetNotFoundException ex) {
            throw new DrsObjectNotFoundException("No study found for DRS object id '" + drsObjectId + "'", ex);
        }
    }

    public DRSBundle lookupBundleByDrsId(String drsBundleId) {
        // TODO: refactor common parts with above?
        DrsId drsId = drsIdService.fromObjectId(drsBundleId);

        try {
            UUID studyId = UUID.fromString(drsId.getStudyId());
            studyDao.retrieveSummaryById(studyId);

            UUID datasetId = UUID.fromString(drsId.getDatasetId());
            datasetDao.retrieveDatasetSummary(datasetId);

            // TODO: validate dataset and check permissions.

            FSObject dirObject = lookupFSObject(
                drsId.getStudyId(),
                drsId.getFsObjectId(),
                FSObject.FSObjectType.DIRECTORY);

            List<FSObject> fsObjectList = fileDao.enumerateDirectory(dirObject);


            // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
            String theTime = dirObject.getCreatedDate().toString();

            DRSBundle bundle = new DRSBundle()
                .id(drsBundleId)
                .name(getLastNameFromPath(dirObject.getPath()))
                .created(theTime)
                .updated(theTime)
                .version(DRS_OBJECT_VERSION)
                .description(dirObject.getDescription())
                .aliases(Collections.singletonList(dirObject.getPath()));

            return makeBundleObjects(bundle, fsObjectList, datasetId.toString());

        } catch (IllegalArgumentException ex) {
            throw new InvalidDrsIdException("Invalid object id format '" + drsBundleId + "'", ex);
        } catch (StudyNotFoundException | DatasetNotFoundException ex) {
            throw new DrsObjectNotFoundException("No study found for DRS object id '" + drsBundleId + "'", ex);
        }
    }

    private DRSBundle makeBundleObjects(DRSBundle bundle, List<FSObject> fsObjectList, String datasetId) {
        // TODO: this computation is not right. Underlying directories should have
        // size and checksums that can be included in this calculation, but ours do not right now.
        // We should be computing these values as files are inserted. In theory, the directory
        // becomes immutable at some point in the process and these values will be stable.
        // That is the idea of giving them checksums in the first place.

        boolean md5IsValid = true;
        List<String> md5List = new ArrayList<>();
        List<String> crc32cList = new ArrayList<>();
        long totalSize = 0;

        for (FSObject fsObject : fsObjectList) {
            String drsUri = drsIdService.toDrsUri(
                fsObject.getStudyId().toString(),
                datasetId,
                fsObject.getObjectId().toString());
            String drsObjectId = drsIdService.toDrsObjectId(
                fsObject.getStudyId().toString(),
                datasetId,
                fsObject.getObjectId().toString());

            DRSBundleObject.TypeEnum objectType = DRSBundleObject.TypeEnum.BUNDLE;

            if (fsObject.getObjectType() == FSObject.FSObjectType.FILE) {
                objectType = DRSBundleObject.TypeEnum.OBJECT;

                if (fsObject.getChecksumMd5() == null) {
                    md5IsValid = false; // can't compute aggregate when not all objects have values
                } else {
                    md5List.add(fsObject.getChecksumMd5());
                }
                crc32cList.add(fsObject.getChecksumCrc32c());
                totalSize += fsObject.getSize();
            }

            DRSBundleObject bundleObject = new DRSBundleObject()
                .name(getLastNameFromPath(fsObject.getPath()))
                .id(drsObjectId)
                .drsUri(Collections.singletonList(drsUri))
                .type(objectType);

            bundle.addContentsItem(bundleObject);
        }

        // Deal with the aggregates
        String crc32cString = "0";
        Collections.sort(crc32cList);
        String allCrc = StringUtils.join(crc32cList);
        if (!allCrc.isEmpty()) {
            byte[] crcbytes = allCrc.getBytes(StandardCharsets.UTF_8);

            PureJavaCrc32C crc32cMaker = new PureJavaCrc32C();
            crc32cMaker.update(crcbytes, 0, crcbytes.length);
            crc32cString = Long.toHexString(crc32cMaker.getValue());
        }
        DRSChecksum drsChecksum = new DRSChecksum().type("crc32c").checksum(crc32cString);
        bundle.addChecksumsItem(drsChecksum);

        if (md5IsValid) {
            Collections.sort(md5List);
            String allMd5 = StringUtils.join(md5List, "");

            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] hashInBytes = md.digest(allMd5.getBytes(StandardCharsets.UTF_8));

                StringBuilder sb = new StringBuilder();
                for (byte b : hashInBytes) {
                    sb.append(String.format("%02x", b));
                }

                DRSChecksum md5Checksum = new DRSChecksum().type("md5").checksum(sb.toString());
                bundle.addChecksumsItem(md5Checksum);
            } catch (NoSuchAlgorithmException ex) {
                logger.warn("No MD5 digest available! Skipped returning an MD5 hash");
            }
        }

        bundle.size(totalSize);

        return bundle;
    }

    private FSObject lookupFSObject(String studyId, String fileId, FSObject.FSObjectType objectType) {
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

    private DRSObject drsObjectFromFSObject(FSObject fsObject, String datasetId) {
        // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
        String theTime = fsObject.getCreatedDate().toString();

        DRSAccessURL accessURL = new DRSAccessURL()
            .url(fsObject.getGspath());

        DRSAccessMethod accessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(accessURL)
            .region(gcsConfiguration.getRegion());

        DrsId drsId = DrsId.builder()
            .studyId(fsObject.getStudyId().toString())
            .datasetId(datasetId)
            .fsObjectId(fsObject.getObjectId().toString())
            .build();

        DRSObject fileModel = new DRSObject()
            .id(drsId.toDrsObjectId())
            .name(getLastNameFromPath(fsObject.getPath()))
            .size(fsObject.getSize())
            .created(theTime)
            .updated(theTime)
            .version(DRS_OBJECT_VERSION)
            .mimeType(fsObject.getMimeType())
            .checksums(makeChecksums(fsObject))
            .accessMethods(Collections.singletonList(accessMethod))
            .description(fsObject.getDescription())
            .aliases(Collections.singletonList(fsObject.getPath()));

        return fileModel;
    }

    private List<DRSChecksum> makeChecksums(FSObject fsObject) {
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

    private String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }



}
