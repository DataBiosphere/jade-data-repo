package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidUriException;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

// Common code for the ingest steps
public final class IngestUtils {
  private IngestUtils() {}

  public static class GsUrlParts {
    private String bucket;
    private String path;
    private boolean isWildcard;

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

    public boolean getIsWildcard() {
      return isWildcard;
    }

    public GsUrlParts isWildcard(boolean isWildcard) {
      this.isWildcard = isWildcard;
      return this;
    }
  }

  public static Dataset getDataset(FlightContext context, DatasetService datasetService) {
    FlightMap inputParameters = context.getInputParameters();
    UUID datasetId =
        UUID.fromString(inputParameters.get(JobMapKeys.DATASET_ID.getKeyName(), String.class));
    return datasetService.retrieve(datasetId);
  }

  public static IngestRequestModel getIngestRequestModel(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
  }

  public static DatasetTable getDatasetTable(FlightContext context, Dataset dataset) {
    IngestRequestModel ingestRequest = getIngestRequestModel(context);
    Optional<DatasetTable> optTable = dataset.getTableByName(ingestRequest.getTable());
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
      throw new InvalidUriException(
          "Ingest source bucket name is not valid in gs: URI: '" + uri + "'");
    }
    if (noGsUri.length() < 4) {
      throw new InvalidUriException(
          "Ingest source file path is not valid in gs: URI: '" + uri + "'");
    }

    String bucket = StringUtils.substringBefore(noGsUri, "/");
    String path = StringUtils.substringAfter(noGsUri, "/");

    if (bucket.indexOf('*') > -1) {
      throw new InvalidUriException("Bucket wildcards are not supported: URI: '" + uri + "'");
    }
    int globIndex = path.indexOf('*');
    boolean isWildcard = globIndex > -1;
    if (isWildcard && path.lastIndexOf('*') != globIndex) {
      throw new InvalidUriException("Multi-wildcards are not supported: URI: '" + uri + "'");
    }

    return new GsUrlParts().bucket(bucket).path(path).isWildcard(isWildcard);
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
    workingMap.put(DatasetWorkingMapKeys.DATASET_NAME, name);
  }

  public static String getDatasetName(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(DatasetWorkingMapKeys.DATASET_NAME, String.class);
  }

  public static void putIngestStatistics(FlightContext context, PdaoLoadStatistics statistics) {
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(IngestMapKeys.INGEST_STATISTICS, statistics);
  }

  public static PdaoLoadStatistics getIngestStatistics(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(IngestMapKeys.INGEST_STATISTICS, PdaoLoadStatistics.class);
  }


  public static List<JsonNode> parse(List<String> source, ObjectMapper objectMapper) {
    return source.stream()
        .map(
            content -> {
              try {
                return objectMapper.readTree(content);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  public static Set<BulkLoadFileModel> collectFilesForIngest(List<JsonNode> fileLineJson,
                                                             List<String> fileRefColumnNames,
                                                             ObjectMapper objectMapper) {
    return fileLineJson.stream()
        .flatMap(
            node ->
                fileRefColumnNames.stream()
                    .map(
                        columnName -> {
                          JsonNode fileRefNode = node.get(columnName);
                          if (fileRefNode.isObject()) {
                            return Optional.of(
                                objectMapper.convertValue(
                                    fileRefNode, bio.terra.model.BulkLoadFileModel.class));
                          } else {
                            return Optional.<bio.terra.model.BulkLoadFileModel>empty();
                          }
                        })
                    .filter(Optional::isPresent)
                    .map(Optional::get))
        .collect(Collectors.toSet());
  }
}
