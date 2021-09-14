package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidBlobURLException;
import bio.terra.service.dataset.exception.InvalidIngestStrategyException;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobUrlParts;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import javax.xml.crypto.Data;

// Common code for the ingest steps
public final class IngestUtils {
  private static final String SOURCE_SCOPED_CREDENTIAL_PREFIX = "source_scoped_credential_";
  private static final String TARGET_SCOPED_CREDENTIAL_PREFIX = "target_scoped_credential_";
  private static final String SOURCE_DATA_SOURCE_PREFIX = "source_data_source_";
  private static final String TARGET_DATA_SOURCE_PREFIX = "target_data_source_";
  private static final String TABLE_NAME_PREFIX = "ingest_";

  private IngestUtils() {}

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

  /**
   * We want to validate and sanitize the ingest source file We are looking for a url that matches
   * the following specification: https://{host}/{container}/{blobName}.csv or
   * https://{host}/{container}/{blobName}.json NOTE: this definition is probably going to be too
   * strict. There are valid blob urls that do not match this specification and we may be able to
   * expand this definition with more testing/requirement gathering
   *
   * @param URL
   * @return
   */
  public static BlobUrlParts validateBlobAzureBlobFileURL(String URL) {
    BlobUrlParts blobUrlParts;
    try {
      blobUrlParts = BlobUrlParts.parse(URL);
    } catch (IllegalArgumentException ex) {
      throw new InvalidBlobURLException("Blob URL parse failed due to malformed URL.", ex);
    }
    validateScheme(blobUrlParts.getScheme(), URL);
    validateHost(blobUrlParts.getHost(), URL);
    validateContainerAndBlobNames(
        blobUrlParts.getBlobContainerName(), blobUrlParts.getBlobName(), URL);
    return blobUrlParts;
  }

  private static void validateScheme(String scheme, String URL) {
    String expectedScheme = "https";
    if (!expectedScheme.equals(scheme)) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + URL
              + "'."
              + "The url is required to use 'https'");
    }
  }

  private static void validateHost(String host, String URL) {
    int separator = StringUtils.indexOf(host, ".");
    String storageAccountName = StringUtils.substring(host, 0, separator);
    if (!storageAccountName.matches("^[a-z0-9]{3,24}")) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + URL
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
              + URL
              + "'. "
              + "The host is expected to take the following format: {storageAccountName}.blob.core.windows.net");
    }
  }

  private static void validateContainerAndBlobNames(
      String containerName, String blobName, String URL) {
    if (!blobName.endsWith(".csv") && !blobName.endsWith(".json")) {
      throw new InvalidBlobURLException(
          "Ingest source is not a valid blob url: '"
              + URL
              + "'. "
              + "The url must include a file name with an extension of either csv or json.");
    }
    String blobNameNoExtension = blobName.substring(0, blobName.lastIndexOf("."));
    List<String> blobNamesToCheck = new ArrayList<>();
    blobNamesToCheck.addAll(Arrays.asList(blobNameNoExtension.split("/")));
    blobNamesToCheck.add(containerName);
    String azureContainerRegex = "^[a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9]$";
    blobNamesToCheck.forEach(
        b -> {
          if (!b.matches(azureContainerRegex)) {
            throw new InvalidBlobURLException(
                "Ingest source is not a valid blob url: '"
                    + URL
                    + "'. "
                    + " The container and each blob name must meet the following requirements: "
                    + "They must start and end with a letter or number. Valid characters include "
                    + "letters, numbers, and the dash (-) character. "
                    + "Every dash (-) character must be immediately preceded and followed by a letter or number; "
                    + "consecutive dashes are not permitted in container names.");
          }
        });
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

  public static final Predicate<FlightContext> noFilesToIngest =
      flightContext -> {
        var numFiles =
            Objects.requireNonNullElse(
                flightContext
                    .getWorkingMap()
                    .get(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, Long.class),
                0L);
        if (numFiles == 0) {
          Logger logger = LoggerFactory.getLogger(flightContext.getFlightClassName());
          logger.info(
              "Skipping {} because there are no files to ingest", flightContext.getStepClassName());
          return true;
        }
        return false;
      };

  public static Stream<JsonNode> getJsonNodesStreamFromFile(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      IngestRequestModel ingestRequest,
      String cloudEncapsulationId,
      List<String> errors) {
    return cloudFileReader
        .getBlobsLinesStream(ingestRequest.getPath(), cloudEncapsulationId)
        .map(
            content -> {
              try {
                return objectMapper.readTree(content);
              } catch (JsonProcessingException ex) {
                errors.add(ex.getMessage());
                return null;
              }
            })
        .filter(Objects::nonNull);
  }

  public static long countBulkFileLoadModelsFromPath(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      IngestRequestModel ingestRequest,
      String cloudEncapsulationId,
      List<String> fileRefColumnNames,
      List<String> errors) {
    try (var nodesStream =
        IngestUtils.getBulkFileLoadModelsStream(
            cloudFileReader,
            objectMapper,
            ingestRequest,
            cloudEncapsulationId,
            fileRefColumnNames,
            errors)) {
      return nodesStream.count();
    }
  }

  public static Stream<BulkLoadFileModel> getBulkFileLoadModelsStream(
      CloudFileReader cloudFileReader,
      ObjectMapper objectMapper,
      IngestRequestModel ingestRequest,
      String cloudEncapsulationId,
      List<String> fileRefColumnNames,
      List<String> errors) {
    return IngestUtils.getJsonNodesStreamFromFile(
            cloudFileReader, objectMapper, ingestRequest, cloudEncapsulationId, errors)
        .flatMap(
            node ->
                fileRefColumnNames.stream()
                    .map(node::get)
                    .filter(n -> n != null && n.isObject())
                    .map(n -> objectMapper.convertValue(n, BulkLoadFileModel.class)));
  }

  public static void checkForLargeIngestRequests(long numLines, long maxIngestRows) {
    if (numLines > maxIngestRows) {
      throw new InvalidIngestStrategyException(
          String.format(
              "The combined file ingest and metadata ingest workflow is limited to "
                  + "%s files for ingest. This request had %s files.",
              maxIngestRows, numLines));
    }
  }

  public static String getParquetTargetLocationURL(
      AzureStorageAccountResource storageAccountResource) {
    String storageAccount = storageAccountResource.getName();
    String storageAccountURLTemplate = "https://<storageAccount>.blob.core.windows.net";

    ST storageAccountURL = new ST(storageAccountURLTemplate);
    storageAccountURL.add("storageAccount", storageAccount);
    return storageAccountURL.render();
  }

  public static String getParquetFilePath(String targetTableName, String flightId) {
    return "parquet/" + targetTableName + "/" + flightId + ".parquet";
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

  public static String getSynapseTableName(String flightId) {
    return TABLE_NAME_PREFIX + flightId;
  }

  public static BillingProfileModel getIngestBillingProfileFromDataset(Dataset dataset, IngestRequestModel ingestRequest) {
    dataset.getDatasetSummary().getBillingProfiles().stream().filter(bp -> bp.getId() == ingestRequest.getProfileId())
        .findFirst().orElseThrow();
  }
}
