package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.FlightUtils;
import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.filedata.exception.BlobAccessNotAuthorizedException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobUrlParts;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.StorageException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Common code for the ingest steps
public final class IngestUtils {
  private static final String SOURCE_DATASET_SCOPED_CREDENTIAL_PREFIX =
      "source_dataset_scoped_credential_";
  private static final String SOURCE_SCOPED_CREDENTIAL_PREFIX = "source_scoped_credential_";
  private static final String TARGET_SCOPED_CREDENTIAL_PREFIX = "target_scoped_credential_";
  private static final String SOURCE_DATASET_DATA_SOURCE_PREFIX = "source_dataset_data_source_";
  private static final String SOURCE_DATA_SOURCE_PREFIX = "source_data_source_";
  private static final String TARGET_DATA_SOURCE_PREFIX = "target_data_source_";
  private static final String INGEST_TABLE_NAME_PREFIX = "ingest_";
  private static final String SCRATCH_TABLE_NAME_PREFIX = "scratch_";
  private static final String FLIGHT_ID_PREFIX = "flight_";
  private static final String PARQUET_PATH_PREFIX = "parquet";

  private static final Logger logger = LoggerFactory.getLogger(IngestUtils.class);

  private IngestUtils() {}

  /**
   * @param context with {@link JobMapKeys#DATASET_ID} provided as an input parameter.
   * @return the dataset ID from the input parameter map
   */
  public static UUID getDatasetId(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    String key = JobMapKeys.DATASET_ID.getKeyName();
    UUID datasetId = inputParameters.get(key, UUID.class);
    if (datasetId == null) {
      throw new IllegalStateException(
          "Expected flight to be called with %s as input parameter".formatted(key));
    }
    return datasetId;
  }

  /**
   * @param context with {@link JobMapKeys#DATASET_ID} provided as an input parameter.
   * @return the existing Dataset object populated only as necessary to facilitate data ingestion
   *     (i.e. without relationships or assets which are constructed via costly database calls).
   */
  public static Dataset getDataset(FlightContext context, DatasetService datasetService) {
    return datasetService.retrieveForIngest(getDatasetId(context));
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

  /**
   * We want to validate and sanitize the ingest source file We are looking for a url that matches
   * the following specification: https://{host}/{container}/{blobName}.csv or
   * https://{host}/{container}/{blobName}.json NOTE: this definition is probably going to be too
   * strict. There are valid blob urls that do not match this specification and we may be able to
   * expand this definition with more testing/requirement gathering
   *
   * @param url
   * @return
   */
  public static BlobUrlParts validateBlobAzureBlobFileURL(String url) {
    BlobUrlParts blobUrlParts;
    try {
      blobUrlParts = BlobUrlParts.parse(url);
    } catch (IllegalArgumentException ex) {
      throw new InvalidBlobURLException("Blob url parse failed due to malformed url.", ex);
    }
    validateScheme(blobUrlParts.getScheme(), url);
    validateHost(blobUrlParts.getHost(), url);
    validateContainerAndBlobNames(
        blobUrlParts.getBlobContainerName(), blobUrlParts.getBlobName(), url);
    return blobUrlParts;
  }

  private static void validateScheme(String scheme, String url) {
    String expectedScheme = "https";
    if (!expectedScheme.equals(scheme)) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + url
              + "'."
              + "The url is required to use 'https'");
    }
  }

  private static void validateHost(String host, String url) {
    int separator = StringUtils.indexOf(host, ".");
    String storageAccountName = StringUtils.substring(host, 0, separator);
    if (!storageAccountName.matches("^[a-z0-9]{3,24}")) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + url
              + "'. "
              + "The host is expected to take the following format: {storageAccountName}.blob.core.windows.net, "
              + "where the storageAccountName must be between 3 and 24 characters in length and "
              + "should consist only of numbers and lowercase letters.");
    }

    String expectedHostURL = ".blob.core.windows.net";
    String actualHostURL = StringUtils.substring(host, separator, host.length());
    if (!actualHostURL.equals(expectedHostURL)) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + url
              + "'. "
              + "The host is expected to take the following format: {storageAccountName}.blob.core.windows.net");
    }
  }

  private static void validateContainerAndBlobNames(
      String containerName, String blobName, String url) {

    String azureContainerRegex = "^[a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9]$";

    if (!containerName.matches(azureContainerRegex)) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + url
              + "'. "
              + "The container must meet the following requirements: "
              + "It must start and end with a letter or number. Valid characters include "
              + "letters, numbers, and the dash (-) character. "
              + "Every dash (-) character must be immediately preceded and followed by a letter or number; "
              + "consecutive dashes are not permitted in container names.");
    }

    if (!blobName.endsWith(".csv") && !blobName.endsWith(".json")) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + url
              + "'. "
              + "The url must include a file name with an extension of either csv or json.");
    }
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

  public static boolean isCombinedFileIngest(FlightContext flightContext) {
    var numFiles =
        Objects.requireNonNullElse(
            flightContext.getWorkingMap().get(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, Long.class),
            0L);
    return numFiles != 0;
  }

  public static boolean isIngestFromPayload(FlightMap inputParameters) {
    IngestRequestModel ingestRequestModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    return ingestRequestModel.getFormat().equals(IngestRequestModel.FormatEnum.ARRAY);
  }

  public static boolean isJsonTypeIngest(FlightMap inputParameters) {
    IngestRequestModel ingestRequestModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
    return ingestRequestModel.getFormat() == IngestRequestModel.FormatEnum.JSON
        || ingestRequestModel.getFormat() == IngestRequestModel.FormatEnum.ARRAY;
  }

  /**
   * @return whether ingest should ignore any user-specified row IDs and generate new ones
   */
  public static boolean shouldIgnoreUserSpecifiedRowIds(FlightMap inputParameters) {
    var updateStrategy =
        inputParameters
            .get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class)
            .getUpdateStrategy();
    // For historical reasons, ingests in append mode may specify their own row IDs.
    return EnumSet.of(
            IngestRequestModel.UpdateStrategyEnum.REPLACE,
            IngestRequestModel.UpdateStrategyEnum.MERGE)
        .contains(updateStrategy);
  }

  public static Stream<JsonNode> getJsonNodesStreamFromFile(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      String path,
      AuthenticatedUserRequest userRequest,
      String cloudEncapsulationId,
      ErrorCollector errorCollector) {
    return cloudFileReader
        .getBlobsLinesStream(path, cloudEncapsulationId, userRequest)
        .map(
            content -> {
              try {
                return objectMapper.readTree(content);
              } catch (JsonProcessingException ex) {
                errorCollector.record("Format error: %s", ex.getMessage());
                return null;
              }
            })
        .filter(Objects::nonNull);
  }

  public static <T> Stream<T> getStreamFromFile(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      String path,
      AuthenticatedUserRequest userRequest,
      String cloudEncapsulationId,
      ErrorCollector errorCollector,
      TypeReference<T> clazz) {
    return cloudFileReader
        .getBlobsLinesStream(path, cloudEncapsulationId, userRequest)
        .map(
            content -> {
              try {
                return objectMapper.readValue(content, clazz);
              } catch (JsonProcessingException ex) {
                errorCollector.record("Format error: %s", ex.getMessage());
                return null;
              }
            })
        .filter(Objects::nonNull);
  }

  /**
   * @return a stream consisting of `nodesStream` elements having first undergone format and access
   *     validation, with any errors recorded to `errorCollector`
   */
  public static Stream<BulkLoadFileModel> validateBulkFileLoadModelsFromStream(
      Stream<BulkLoadFileModel> nodesStream,
      CloudFileReader cloudFileReader,
      String cloudEncapsulationId,
      AuthenticatedUserRequest userRequest,
      ErrorCollector errorCollector,
      Dataset dataset) {
    return nodesStream.peek(
        loadFileModel -> {
          try {
            validateBulkLoadFileModel(loadFileModel);
            cloudFileReader.validateUserCanRead(
                List.of(loadFileModel.getSourcePath()), cloudEncapsulationId, userRequest, dataset);
          } catch (BlobAccessNotAuthorizedException
              | BadRequestException
              | IllegalArgumentException
              | StorageException ex) {
            errorCollector.record("Error: %s", ex.getMessage());
          }
        });
  }

  public static long validateAndCountBulkFileLoadModelsFromPath(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      IngestRequestModel ingestRequest,
      AuthenticatedUserRequest userRequest,
      String cloudEncapsulationId,
      List<Column> fileRefColumns,
      ErrorCollector errorCollector,
      Dataset dataset) {
    try (var nodesStream =
        IngestUtils.getBulkFileLoadModelsStream(
            cloudFileReader,
            objectMapper,
            ingestRequest,
            userRequest,
            cloudEncapsulationId,
            fileRefColumns,
            errorCollector)) {
      return IngestUtils.validateBulkFileLoadModelsFromStream(
              nodesStream,
              cloudFileReader,
              cloudEncapsulationId,
              userRequest,
              errorCollector,
              dataset)
          .count();
    }
  }

  public static Stream<BulkLoadFileModel> getBulkFileLoadModelsStream(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      IngestRequestModel ingestRequest,
      AuthenticatedUserRequest userRequest,
      String cloudEncapsulationId,
      List<Column> fileRefColumns,
      ErrorCollector errorCollector) {
    return IngestUtils.getJsonNodesStreamFromFile(
            cloudFileReader,
            objectMapper,
            ingestRequest.getPath(),
            userRequest,
            cloudEncapsulationId,
            errorCollector)
        .flatMap(
            node ->
                fileRefColumns.stream()
                    .map(Column::getName)
                    .map(node::get)
                    .filter(Objects::nonNull)
                    .flatMap(
                        n -> {
                          if (n.isObject()) {
                            return Stream.of(n);
                          } else if (n.isArray()) {
                            return StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(n.iterator(), 0), false)
                                .filter(JsonNode::isObject);
                          } else {
                            return Stream.empty();
                          }
                        })
                    .map(
                        n -> {
                          try {
                            return objectMapper.convertValue(n, BulkLoadFileModel.class);
                          } catch (IllegalArgumentException ex) {
                            errorCollector.record("Error: %s", ex.getMessage());
                          }
                          return null;
                        }))
        .distinct()
        .filter(Objects::nonNull);
  }

  public static List<Column> getDatasetFileRefColumns(
      Dataset dataset, IngestRequestModel ingestRequest) {
    return dataset.getTableByName(ingestRequest.getTable()).orElseThrow().getColumns().stream()
        .filter(c -> c.getType() == TableDataType.FILEREF)
        .collect(Collectors.toList());
  }

  // Note: this is the unqualified path (e.g. it gets used in metadata and scratch directories)
  public static String getParquetFilePath(String targetTableName, String flightId) {
    return PARQUET_PATH_PREFIX
        + "/"
        + targetTableName
        + "/"
        + FLIGHT_ID_PREFIX
        + flightId
        + ".parquet";
  }

  /**
   * Returns wildcard path that can be used to retrieve all data for a snapshot table A snapshot
   * table can be represented by multiple parquet files in a snapshot table directory
   *
   * @param targetTableName
   * @return
   */
  public static String getSnapshotParquetFilePathForQuery(String targetTableName) {
    return FolderType.METADATA.getPath(
        PARQUET_PATH_PREFIX + "/" + targetTableName + "/*.parquet/*");
  }

  /**
   * Returns full path and name of a parquet file representing a slice of a snapshot. There can be
   * multiple slices/parquet files per snapshot table. All parquet files within the table directory
   * represent the data in a snapshot table.
   *
   * @param targetTableName Snapshot table name that will be used as a directory
   * @param snapshotSliceName Name of parquet file
   * @return
   */
  public static String getSnapshotSliceParquetFilePath(
      String targetTableName, String snapshotSliceName) {
    return FolderType.METADATA.getPath(
        PARQUET_PATH_PREFIX + "/" + targetTableName + "/" + snapshotSliceName + ".parquet");
  }

  public static String getSourceDatasetParquetFilePath(String tableName) {
    return FolderType.METADATA.getPath(PARQUET_PATH_PREFIX + "/" + tableName + "/*/*.parquet");
  }

  public static String formatSnapshotTableName(UUID snapshotId, String tableName) {
    return snapshotId.toString().replaceAll("-", "") + "_" + tableName;
  }

  public static String getSourceDatasetDataSourceName(String flightId) {
    return SOURCE_DATASET_DATA_SOURCE_PREFIX + flightId;
  }

  public static String getSourceDatasetScopedCredentialName(String flightId) {
    return SOURCE_DATASET_SCOPED_CREDENTIAL_PREFIX + flightId;
  }

  public static String getIngestRequestDataSourceName(String flightId) {
    return SOURCE_DATA_SOURCE_PREFIX + flightId;
  }

  public static String getIngestRequestScopedCredentialName(String flightId) {
    return SOURCE_SCOPED_CREDENTIAL_PREFIX + flightId;
  }

  public static String getTargetDataSourceName(String flightId) {
    return TARGET_DATA_SOURCE_PREFIX + flightId;
  }

  public static String getTargetScopedCredentialName(String flightId) {
    return TARGET_SCOPED_CREDENTIAL_PREFIX + flightId;
  }

  public static String getSynapseIngestTableName(String flightId) {
    return INGEST_TABLE_NAME_PREFIX + flightId;
  }

  public static String getSynapseScratchTableName(String flightId) {
    return SCRATCH_TABLE_NAME_PREFIX + flightId;
  }

  public static BillingProfileModel getIngestBillingProfileFromDataset(
      Dataset dataset, IngestRequestModel ingestRequest) {
    return dataset.getDatasetSummary().getBillingProfiles().stream()
        .filter(bp -> bp.getId().equals(ingestRequest.getProfileId()))
        .findFirst()
        .orElseThrow();
  }

  public static void deleteScratchFile(FlightContext context, GcsPdao gcsPdao) {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource =
        FlightUtils.getTyped(workingMap, CommonFlightKeys.SCRATCH_BUCKET_INFO);
    String pathToIngestFile = workingMap.get(IngestMapKeys.INGEST_CONTROL_FILE_PATH, String.class);
    gcsPdao.deleteFileByGspath(pathToIngestFile, bucketResource.projectIdForBucket());
  }

  public static void deleteLandingFile(FlightContext context, GcsPdao gcsPdao) {
    FlightMap workingMap = context.getWorkingMap();
    GoogleBucketResource bucketResource =
        FlightUtils.getTyped(workingMap, CommonFlightKeys.SCRATCH_BUCKET_INFO);
    if (bucketResource != null) {
      IngestRequestModel ingestRequestModel =
          context
              .getInputParameters()
              .get(JobMapKeys.REQUEST.getKeyName(), IngestRequestModel.class);
      String pathToLandingFile = ingestRequestModel.getPath();
      gcsPdao.deleteFileByGspath(pathToLandingFile, bucketResource.projectIdForBucket());
    } else {
      // Occurs when there is an "array" combined ingest
      logger.info("No scratch bucket to delete");
    }
  }

  public static void validateBulkLoadFileModel(BulkLoadFileModel loadFile) {
    List<String> itemsNotDefined = new ArrayList<>();
    if (StringUtils.isEmpty(loadFile.getSourcePath())) {
      itemsNotDefined.add("sourcePath");
    }
    if (StringUtils.isEmpty(loadFile.getTargetPath())) {
      itemsNotDefined.add("targetPath");
    }
    if (itemsNotDefined.size() > 0) {
      throw new BadRequestException(
          String.format(
              "The following required field(s) were not defined: %s",
              itemsNotDefined.stream().collect(Collectors.joining(", "))));
    }
  }

  public static String getDataSourceName(String flightId) {
    return IngestUtils.getTargetDataSourceName(flightId);
  }

  public static String getScopedCredentialName(String flightId) {
    return IngestUtils.getTargetScopedCredentialName(flightId);
  }
}
