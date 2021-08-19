package bio.terra.common;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class SynapseUtils {
  private static final Logger logger = LoggerFactory.getLogger(SynapseUtils.class);
  private static final String readFromParquetFile =
      "SELECT *\n"
          + "FROM OPENROWSET(\n"
          + "    BULK '<parquetFilePath>',\n"
          + "    DATA_SOURCE = '<dataSourceName>',\n"
          + "    FORMAT = 'parquet') as rows";

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;

  public List<String> readParquetFileStringColumn(
      String parquetFilePath, String dataSourceName, String columnName, boolean expectSuccess) {
    ST sqlReadTemplate = new ST(readFromParquetFile);
    sqlReadTemplate.add("parquetFilePath", parquetFilePath);
    sqlReadTemplate.add("dataSourceName", dataSourceName);
    SQLServerDataSource ds = azureSynapsePdao.getDatasource();
    List<String> resultList = new ArrayList<>();
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlReadTemplate.render())) {
      while (rs.next()) {
        resultList.add(rs.getString(columnName));
      }
    } catch (SQLException ex) {
      if (expectSuccess) {
        logger.error("Unable to read parquet file.", ex);
      }
    }
    return resultList;
  }

  public void deleteParquetFile(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccount,
      String parquetFileName) {
    BlobContainerClientFactory targetDataClientFactory =
        azureBlobStorePdao.getTargetDataClientFactory(
            profileModel, storageAccount, AzureStorageAccountResource.ContainerType.METADATA, true);

    var result =
        targetDataClientFactory
            .getBlobContainerClient()
            .listBlobsByHierarchy(parquetFileName + "/");
    result.forEach(
        s -> {
          if (s.isPrefix() == null || !s.isPrefix()) {
            logger.info("Attempting to delete the parquet blob: {}", s.getName());
            targetDataClientFactory.getBlobContainerClient().getBlobClient(s.getName()).delete();
          }
        });
    logger.info("Attempting to delete the parquet directory {}", parquetFileName);
    targetDataClientFactory.getBlobContainerClient().getBlobClient(parquetFileName).delete();
  }

  public String ingestRequestURL(
      String storageAccountName, String ingestRequestContainer, String fileName) {
    String sqlTemplate =
        "https://<storageAccountName>.blob.core.windows.net/<ingestRequestContainer>/<requestFileName>";
    ST urlTemplate = new ST(sqlTemplate);
    urlTemplate.add("storageAccountName", storageAccountName);
    urlTemplate.add("ingestRequestContainer", ingestRequestContainer);
    urlTemplate.add("requestFileName", fileName);
    return urlTemplate.render();
  }
}
