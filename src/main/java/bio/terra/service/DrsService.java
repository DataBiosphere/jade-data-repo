package bio.terra.service;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.dao.exception.StudyNotFoundException;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

// TODO: NOTE: This code implements an out-of-date version of the DRS spec. See DR-409.
// It probably works, but is not useful.
@Component
public class DrsService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.DrsService");

    private static final String DRS_OBJECT_VERSION = "0";

    private final StudyDao studyDao;
    private final DatasetDao datasetDao;
    private final FireStoreFileDao fileDao;
    private final FileService fileService;
    private final DrsIdService drsIdService;
    private final GcsConfiguration gcsConfiguration;
    private final StudyService studyService;

    @Autowired
    public DrsService(StudyDao studyDao,
                      DatasetDao datasetDao,
                      FireStoreFileDao fileDao,
                      FileService fileService,
                      DrsIdService drsIdService,
                      GcsConfiguration gcsConfiguration,
                      StudyService studyService) {
        this.studyDao = studyDao;
        this.datasetDao = datasetDao;
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.drsIdService = drsIdService;
        this.gcsConfiguration = gcsConfiguration;
        this.studyService = studyService;
    }

    public DRSObject lookupObjectByDrsId(String drsObjectId, Boolean expand) {
        // TODO: Implement recursive directory expansion
        if (expand) {
            throw new NotImplementedException("Expand is not yet implemented");
        }

        DrsId drsId = parseAndValidateDrsId(drsObjectId);

        FSObjectBase fsObject = fileService.lookupFSObject(
            drsId.getStudyId(),
            drsId.getFsObjectId());

        switch (fsObject.getObjectType()) {
            case FILE:
                return drsObjectFromFSFile((FSFile)fsObject, drsId.getDatasetId());

            case DIRECTORY:
                return drsObjectFromFSDir((FSDir)fsObject, drsId.getDatasetId());

            default:
                throw new IllegalArgumentException("Invalid object type");
        }
    }

    private DRSObject drsObjectFromFSFile(FSFile fsFile, String datasetId) {
        DRSObject fileObject = makeCommonDrsObject(fsFile, datasetId);

        DRSAccessURL accessURL = new DRSAccessURL()
            .url(fsFile.getGspath());

        DRSAccessMethod accessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(accessURL)
            .region(gcsConfiguration.getRegion());

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

        // If the object is an enumerated directory, we fill in the contents array.
        if (fsObject.getObjectType() == FSObjectType.DIRECTORY) {
            FSDir fsDir = (FSDir) fsObject;
            if (fsDir.isEnumerated()) {
                contentsObject.contents(makeContentsList(fsDir, datasetId));
            }
        }

        return contentsObject;
    }

    private DrsId makeDrsId(FSObjectBase fsObject, String datasetId) {
        return DrsId.builder()
            .studyId(fsObject.getStudyId().toString())
            .datasetId(datasetId)
            .fsObjectId(fsObject.getObjectId().toString())
            .build();
    }

    private String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

    // Take an object or bundle id. Make sure it parses and make sure that the study and dataset
    // that it claims to be part of actually exist.
    // TODO: add permission checking here I think
    private DrsId parseAndValidateDrsId(String drsObjectId) {
        DrsId drsId = drsIdService.fromObjectId(drsObjectId);
        try {
            UUID studyId = UUID.fromString(drsId.getStudyId());
            studyDao.retrieveSummaryById(studyId);

            UUID datasetId = UUID.fromString(drsId.getDatasetId());
            datasetDao.retrieveDatasetSummary(datasetId);

            return drsId;
        } catch (IllegalArgumentException ex) {
            throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
        } catch (StudyNotFoundException ex) {
            throw new DrsObjectNotFoundException("No study found for DRS object id '" + drsObjectId + "'", ex);
        } catch (DatasetNotFoundException ex) {
            throw new DrsObjectNotFoundException("No dataset found for DRS object id '" + drsObjectId + "'", ex);
        }
    }

/*

    public DRSBundle lookupBundleByDrsId(String drsBundleId) {
        DrsId drsId = parseAndValidateDrsId(drsBundleId);
        Study study = studyService.retrieve(UUID.fromString(drsId.getStudyId()));
        FSObjectBase fsObject = fileDao.retrieveWithContents(
            study,
            UUID.fromString(drsId.getFsObjectId()));
        if (fsObject.getObjectType() != FSObjectType.DIRECTORY) {
            throw new IllegalArgumentException("Object is not a bundle");
        }
        FSDir dirObject = (FSDir)fsObject;
        List<FSObjectBase> fsObjectList = dirObject.getContents();

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

        return makeBundleObjects(bundle, fsObjectList, drsId.getDatasetId());
    }

    private DRSBundle makeBundleObjects(DRSBundle bundle, List<FSObjectBase> fsObjectList, String datasetId) {
        // TODO: this computation does not conform to the current spec. With fine-grain access
        // control, we cannot pre-compute the sizes or checksums of contained bundles. I have raised
        // the question in the GA4GH cloud stream.
        boolean md5IsValid = true;
        List<String> md5List = new ArrayList<>();
        List<String> crc32cList = new ArrayList<>();
        long totalSize = 0;

        for (FSObjectBase fsObject : fsObjectList) {
            String drsUri = drsIdService.toDrsUri(
                fsObject.getStudyId().toString(),
                datasetId,
                fsObject.getObjectId().toString());
            String drsObjectId = drsIdService.toDrsObjectId(
                fsObject.getStudyId().toString(),
                datasetId,
                fsObject.getObjectId().toString());

            DRSBundleObject.TypeEnum objectType = DRSBundleObject.TypeEnum.BUNDLE;

            if (fsObject.getObjectType() == FSObjectType.FILE) {
                FSFile fsFile = (FSFile)fsObject;
                objectType = DRSBundleObject.TypeEnum.OBJECT;

                if (fsFile.getChecksumMd5() == null) {
                    md5IsValid = false; // can't compute aggregate when not all objects have values
                } else {
                    md5List.add(fsFile.getChecksumMd5());
                }
                crc32cList.add(fsFile.getChecksumCrc32c());
                totalSize += fsFile.getSize();
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

 */

}
