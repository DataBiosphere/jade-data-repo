package scripts.utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.BulkLoadFileResultModel;
import bio.terra.datarepo.model.BulkLoadResultModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.JobModel;
import com.azure.resourcemanager.AzureResourceManager;
import com.google.cloud.storage.BlobId;
import common.utils.BlobIOTestUtility;
import common.utils.FileUtils;
import common.utils.StorageUtils;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServiceAccountSpecification;
import runner.config.TestUserSpecification;

public class BulkLoadUtils {
  private static final Logger logger = LoggerFactory.getLogger(BulkLoadUtils.class);

  // This should run about 5 minutes on 2 DRmanager instances. The speed of loads is when
  // they are not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do
  // 2.5 files per minute, so two instances should do 5 files per minute. To run 5 minutes we should
  // run 25 files.
  public static BulkLoadArrayRequestModel buildBulkLoadFileRequest(
      int filesToLoad, UUID billingProfileId) {
    String loadTag = FileUtils.randomizeName("longtest");

    return buildLoadArray(
        filesToLoad,
        billingProfileId,
        loadTag,
        26,
        "gs://jade-testdata-uswestregion/fileloadscaletest",
        "/file1GB-%02d.txt");
  }

  // This bulk load should be short, with small files and stored locally.
  public static BulkLoadArrayRequestModel buildBulkLoadFileRequest100B(
      int filesToLoad, UUID billingProfileId) {
    String loadTag = FileUtils.randomizeName("100Btest");

    return buildLoadArray(
        filesToLoad,
        billingProfileId,
        loadTag,
        26,
        "gs://jade-testdata/loadtest",
        "/file100B-%02d.txt");
  }

  public static BulkLoadArrayRequestModel buildAzureBulkLoadFileRequest100B(
      int filesToLoad,
      UUID billingProfileId,
      BlobIOTestUtility blobIOTestUtility,
      AzureResourceManager client) {
    String loadTag = FileUtils.randomizeName("100Btest");

    return buildAzureLoadArray(
        filesToLoad,
        billingProfileId,
        loadTag,
        26,
        "https://tdrtestdatauseast.blob.core.windows.net/testrunner-data",
        "/file100B-%02d.txt",
        blobIOTestUtility,
        client);
  }

  private static BulkLoadArrayRequestModel buildLoadArray(
      int filesToLoad,
      UUID billingProfileId,
      String loadTag,
      int numberOfSourceFiles,
      String sourcePrefix,
      String fileFormat) {

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(billingProfileId)
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    for (int i = 0; i < filesToLoad; i++) {
      String sourcePath = sourcePrefix + String.format(fileFormat, i % numberOfSourceFiles);
      String targetPath = "/" + loadTag + String.format(fileFormat, i);
      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    return arrayLoad;
  }

  private static BulkLoadArrayRequestModel buildAzureLoadArray(
      int filesToLoad,
      UUID billingProfileId,
      String loadTag,
      int numberOfSourceFiles,
      String sourcePrefix,
      String fileFormat,
      BlobIOTestUtility blobIOTestUtility,
      AzureResourceManager client) {

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(billingProfileId)
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    String key =
        blobIOTestUtility.getSourceStorageAccountPrimarySharedKey(
            client, "TDR_data", "tdrtestdatauseast");
    for (int i = 0; i < filesToLoad; i++) {
      String sourcePath = sourcePrefix + String.format(fileFormat, i % numberOfSourceFiles);
      String sourceFile = String.format("file100B-%02d.txt", i % numberOfSourceFiles);
      String signedSourcePath =
          String.format(
              "%s?%s",
              sourcePath,
              blobIOTestUtility.generateBlobSasTokenWithReadPermissions(key, sourceFile));
      String targetPath = "/" + loadTag + String.format(fileFormat, i);
      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    return arrayLoad;
  }

  public static BulkLoadResultModel getAndDisplayResults(
      RepositoryApi repositoryApi,
      JobModel bulkLoadArrayJobResponse,
      TestUserSpecification testUser)
      throws Exception {
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse, testUser);

    BulkLoadArrayResultModel result =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, bulkLoadArrayJobResponse, BulkLoadArrayResultModel.class);

    BulkLoadResultModel loadSummary = result.getLoadSummary();

    logger.debug("Total files    : {}", loadSummary.getTotalFiles());
    logger.debug("Succeeded files: {}", loadSummary.getSucceededFiles());
    logger.debug("Failed files   : {}", loadSummary.getFailedFiles());
    logger.debug("Not Tried files: {}", loadSummary.getNotTriedFiles());

    return loadSummary;
  }

  // Given the result of an array bulk load, generate a scratch file to use for loading
  // into the simple dataset
  public static BlobId writeScratchFileForIngestRequest(
      ServiceAccountSpecification serviceAccount,
      BulkLoadArrayResultModel arrayResultModel,
      String bucketName,
      String fileName)
      throws Exception {

    String jsonLine =
        "{\"VCF_File_Name\":\"%s\", \"Description\":\"%s\", \"VCF_File_Ref\":\"%s\"}%n";
    StringBuilder sb = new StringBuilder();

    for (BulkLoadFileResultModel fileResult : arrayResultModel.getLoadFileResults()) {
      sb.append(
          String.format(
              jsonLine, fileResult.getTargetPath(), fileResult.getState(), fileResult.getFileId()));
    }

    byte[] fileRefBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
    return StorageUtils.writeBytesToFile(
        StorageUtils.getClientForServiceAccount(serviceAccount),
        bucketName,
        fileName,
        fileRefBytes);
  }

  public static BlobId writeScratchFileForCombinedIngestRequest(
      ServiceAccountSpecification serviceAccount,
      BulkLoadFileModel fileModel,
      String bucketName,
      String ingestFileName)
      throws Exception {
    String jsonLine = "{\"VCF_File_Name\":\"%s\", \"Description\":\"%s\", \"VCF_File_Ref\":%s}%n";
    String scratchFileJson =
        String.format(
            jsonLine, "testFile", "combined metadata ingest", bulkLoadFileModelToJson(fileModel));
    byte[] fileRefBytes = scratchFileJson.getBytes(StandardCharsets.UTF_8);
    return StorageUtils.writeBytesToFile(
        StorageUtils.getClientForServiceAccount(serviceAccount),
        bucketName,
        ingestFileName,
        fileRefBytes);
  }

  private static String bulkLoadFileModelToJson(BulkLoadFileModel fileModel) {
    String fileJSON =
        "{\"description\":\"Test file\", \"mimeType\":\"%s\", \"sourcePath\":\"%s\", \"targetPath\": \"%s\"}";
    return String.format(
        fileJSON, fileModel.getMimeType(), fileModel.getSourcePath(), fileModel.getTargetPath());
  }

  public static String azureWriteScratchFileForIngestRequest(
      BlobIOTestUtility blobIOTestUtility,
      BulkLoadArrayResultModel arrayResultModel,
      String fileName)
      throws Exception {

    String jsonLine =
        "{\"VCF_File_Name\":\"%s\", \"Description\":\"%s\", \"VCF_File_Ref\":\"%s\"}%n";
    StringBuilder sb = new StringBuilder();

    for (BulkLoadFileResultModel fileResult : arrayResultModel.getLoadFileResults()) {
      sb.append(
          String.format(
              jsonLine, fileResult.getTargetPath(), fileResult.getState(), fileResult.getFileId()));
    }
    return blobIOTestUtility.uploadFileWithContents(fileName, sb.toString());
  }

  // Given a scratch file to use for loading into the simple dataset, make the associated ingest
  // request model
  public static IngestRequestModel makeIngestRequestFromScratchFile(BlobId scratchFile) {
    String gsPath = StorageUtils.blobIdToGSPath(scratchFile);

    return new IngestRequestModel()
        .format(IngestRequestModel.FormatEnum.JSON)
        .table("vcf_file")
        .path(gsPath);
  }
}
