package bio.terra.service;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.DatasetNotFoundException;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.FSObject;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSBundle;
import bio.terra.model.DRSBundleObject;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.pdao.gcs.GcsConfiguration;
import bio.terra.service.exception.DrsObjectNotFoundException;
import bio.terra.service.exception.InvalidDrsIdException;
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

    @Autowired
    public DrsService(StudyDao studyDao,
                      DatasetDao datasetDao,
                      FireStoreFileDao fileDao,
                      FileService fileService,
                      DrsIdService drsIdService,
                      GcsConfiguration gcsConfiguration) {
        this.studyDao = studyDao;
        this.datasetDao = datasetDao;
        this.fileDao = fileDao;
        this.fileService = fileService;
        this.drsIdService = drsIdService;
        this.gcsConfiguration = gcsConfiguration;
    }

    public DRSObject lookupObjectByDrsId(String drsObjectId) {
        DrsId drsId = parseAndValidateDrsId(drsObjectId);

        FSObject fsObject = fileService.lookupFSObject(
            drsId.getStudyId(),
            drsId.getFsObjectId(),
            FSObject.FSObjectType.FILE);
        return drsObjectFromFSObject(fsObject, drsId.getDatasetId());
    }

    public DRSBundle lookupBundleByDrsId(String drsBundleId) {
        DrsId drsId = parseAndValidateDrsId(drsBundleId);

        FSObject dirObject = fileDao.retrieve(
            UUID.fromString(drsId.getStudyId()),
            UUID.fromString(drsId.getFsObjectId()));

        List<FSObject> fsObjectList = fileDao.enumerateDirectory(dirObject.getStudyId(), dirObject.getObjectId());

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

    private DRSBundle makeBundleObjects(DRSBundle bundle, List<FSObject> fsObjectList, String datasetId) {
        // TODO: this computation does not conform to the current spec. With fine-grain access
        // control, we cannot pre-compute the sizes or checksums of contained bundles. I have raised
        // the question in the GA4GH cloud stream.
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
            .checksums(fileService.makeChecksums(fsObject))
            .accessMethods(Collections.singletonList(accessMethod))
            .description(fsObject.getDescription())
            .aliases(Collections.singletonList(fsObject.getPath()));

        return fileModel;
    }

    private String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

}
