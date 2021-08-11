package bio.terra.common;

import bio.terra.service.filedata.azure.AzureSynapsePdao;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
public class SynapseUtils {
  @Autowired AzureSynapsePdao azureSynapsePdao;
  private static final String readFromParquetFile =
      "SELECT *\n"
          + "FROM OPENROWSET(\n"
          + "    BULK '<parquetFilePath>',\n"
          + "    DATA_SOURCE = '<dataSourceName>',\n"
          + "    FORMAT = 'parquet') as rows";

  public List<String> readParquetFileStringColumn(
      String parquetFilePath, String dataSourceName, String columnName) throws SQLException {
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
    }
    return resultList;
  }
}
