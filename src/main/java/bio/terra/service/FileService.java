package bio.terra.service;

import bio.terra.dao.DatasetDao;
import bio.terra.dao.StudyDao;
import bio.terra.flight.file.delete.FileDeleteFlight;
import bio.terra.flight.file.ingest.FileIngestFlight;
import bio.terra.metadata.FSObject;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.gcs.GcsConfiguration;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class FileService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.FileService");

    private final Stairway stairway;
    private final StudyDao studyDao;
    private final DatasetDao datasetDao;
    private final GcsConfiguration gcsConfiguration;

    @Autowired
    public FileService(Stairway stairway,
                       StudyDao studyDao,
                       DatasetDao datasetDao,
                       GcsConfiguration gcsConfiguration) {
        this.stairway = stairway;
        this.studyDao = studyDao;
        this.datasetDao = datasetDao;
        this.gcsConfiguration = gcsConfiguration;
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

    public DRSObject fileModelFromFSObject(FSObject fsObject) {
        // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
        OffsetDateTime theTime = OffsetDateTime.ofInstant(fsObject.getCreatedDate(), ZoneId.of("Z"));

        DRSAccessURL accessURL = new DRSAccessURL()
            .url(fsObject.getGspath());

        DRSAccessMethod accessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(accessURL)
            .region(gcsConfiguration.getRegion());

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

        DRSObject fileModel = new DRSObject()
            .id(fsObject.getObjectId().toString())
            .name(getLastNameFromPath(fsObject.getPath()))
            .size(fsObject.getSize())
            .created(theTime)
            .updated(theTime)
            .version("1")
            .mimeType(fsObject.getMimeType())
            .checksums(checksums)
            .accessMethods(Collections.singletonList(accessMethod))
            .description(fsObject.getDescription())
            .aliases(Collections.singletonList(fsObject.getGspath()));

        return fileModel;
    }

    // TODO: Maybe this should go in a path utils class
    private String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

}
