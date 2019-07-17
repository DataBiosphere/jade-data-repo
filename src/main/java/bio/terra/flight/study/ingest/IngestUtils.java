package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.flight.exception.InvalidUriException;
import bio.terra.flight.exception.TableNotFoundException;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.UUID;

// Common code for the ingest steps
public final class IngestUtils {
    private IngestUtils() {
    }

    public static class GsUrlParts {
        private String bucket;
        private String path;

        public String getBucket() {
            return bucket;
        }

        public GsUrlParts bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public String getPath() {
            return path;
        }

        public GsUrlParts path(String path) {
            this.path = path;
            return this;
        }
    }

    public static Study getStudy(FlightContext context, StudyDao studyDao) {
        FlightMap inputParameters = context.getInputParameters();
        String id = inputParameters.get(IngestMapKeys.STUDY_ID, String.class);
        UUID studyId = UUID.fromString(id);
        return studyDao.retrieve(studyId);
    }

    public static IngestRequestModel getIngestRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    }

    public static Table getStudyTable(FlightContext context, Study study) {
        IngestRequestModel ingestRequest = getIngestRequestModel(context);
        Optional<Table> optTable = study.getTableByName(ingestRequest.getTable());
        if (!optTable.isPresent()) {
            throw new TableNotFoundException("Table not found: " + ingestRequest.getTable());
        }
        return optTable.get();
    }

    public static GsUrlParts parseBlobUri(String uri) {
        String protocol = "gs://";
        if (!StringUtils.startsWith(uri, protocol)) {
            throw new InvalidUriException("Ingest source is not a valid gs: URI: '" + uri + "'");
        }
        String noGsUri = StringUtils.substring(uri, protocol.length());
        int firstSlash = StringUtils.indexOf(uri, "/", protocol.length());
        if (firstSlash < 2) {
            throw new InvalidUriException("Ingest source bucket name is not valid in gs: URI: '" + uri + "'");
        }
        if (noGsUri.length() < 4) {
            throw new InvalidUriException("Ingest source file path is not valid in gs: URI: '" + uri + "'");
        }
        return new GsUrlParts()
            .bucket(StringUtils.substringBefore(noGsUri, "/"))
            .path(StringUtils.substringAfter(noGsUri, "/"));
    }

    public static void putStagingTableName(FlightContext context, String name) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(IngestMapKeys.STAGING_TABLE_NAME, name);
    }

    public static String getStagingTableName(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        return workingMap.get(IngestMapKeys.STAGING_TABLE_NAME, String.class);
    }

    public static void putStudyName(FlightContext context, String name) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(IngestMapKeys.STUDY_NAME, name);
    }

    public static String getStudyName(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        return workingMap.get(IngestMapKeys.STUDY_NAME, String.class);
    }

    public static void putIngestStatistics(FlightContext context, PdaoLoadStatistics statistics) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(IngestMapKeys.INGEST_STATISTICS, statistics);
    }

    public static PdaoLoadStatistics getIngestStatistics(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        return workingMap.get(IngestMapKeys.INGEST_STATISTICS, PdaoLoadStatistics.class);
    }

}
