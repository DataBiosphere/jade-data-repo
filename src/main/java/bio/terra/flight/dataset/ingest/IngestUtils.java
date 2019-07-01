package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.flight.exception.InvalidUriException;
import bio.terra.flight.exception.TableNotFoundException;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
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

    public static DrDataset getDataset(FlightContext context, DrDatasetDao datasetDao) {
        FlightMap inputParameters = context.getInputParameters();
        String id = inputParameters.get(IngestMapKeys.DATASET_ID, String.class);
        UUID datasetId = UUID.fromString(id);
        return datasetDao.retrieve(datasetId);
    }

    public static IngestRequestModel getIngestRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    }

    public static Table getDatasetTable(FlightContext context, DrDataset dataset) {
        IngestRequestModel ingestRequest = getIngestRequestModel(context);
        Optional<Table> optTable = dataset.getTableByName(ingestRequest.getTable());
        if (!optTable.isPresent()) {
            throw new TableNotFoundException("Table not found: " + ingestRequest.getTable());
        }
        return optTable.get();
    }

    public static boolean deleteStagingTable(FlightContext context, BigQueryPdao bigQueryPdao) {
        String datasetName = IngestUtils.getDatasetName(context);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        return bigQueryPdao.deleteTable(datasetName, stagingTableName);
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

    public static void putDatasetName(FlightContext context, String name) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(IngestMapKeys.DATASET_NAME, name);
    }

    public static String getDatasetName(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        return workingMap.get(IngestMapKeys.DATASET_NAME, String.class);
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
