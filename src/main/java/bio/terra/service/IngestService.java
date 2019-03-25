package bio.terra.service;

import bio.terra.dao.StudyDao;
import bio.terra.flight.study.ingest.IngestMapKeys;
import bio.terra.flight.study.ingest.StudyIngestFlight;
import bio.terra.model.IngestRequestModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Component
public class IngestService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.DatasetService");

    private final Stairway stairway;
    private final StudyDao studyDao;

    @Autowired
    public IngestService(Stairway stairway,
                         StudyDao studyDao) {
        this.stairway = stairway;
        this.studyDao = studyDao;
    }

    public String ingestStudy(String id, IngestRequestModel ingestRequestModel) {
        // Fill in a default load id if the caller did not provide one in the ingest request.
        if (StringUtils.isEmpty(ingestRequestModel.getLoadTag())) {
            String loadTag = "load-at-" + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
            ingestRequestModel.setLoadTag(loadTag);
        }

        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(),
            "Ingest from " + ingestRequestModel.getPath() +
                " to " + ingestRequestModel.getTable() +
                " in study id " + id);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), ingestRequestModel);
        flightMap.put(IngestMapKeys.STUDY_ID, id);
        return stairway.submit(StudyIngestFlight.class, flightMap);
    }

}
