package bio.terra.service.filedata.azure;

import bio.terra.common.category.Connected;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);
  @Autowired AzureSynapsePdao azureSynapsePdao;

  @Test
  public void testSynapseQuery() throws SQLException {
    // basic test
    boolean success = azureSynapsePdao.runAQuery("Select * FROM ourCoolTable;");
    logger.info("After query. {}", success);

    // ingest dataset steps

    String ingestFileLocation =
        "https://tdrsynapse1.blob.core.windows.net/shelbycontainerexample"
            + "?sp=racwdlmeop&st=2021-07-23T20:04:26Z&se=2021-07-24T04:04:26Z&spr=https"
            + "&sv=2020-08-04&sr=c&sig=NNvOV5LuCUhy0KgZrSn13mp99QFqHYa9Hr4tAeuoE6I%3D";
    String ingestFileName = "example.csv";

    // 1 - Create Sas token for ingest control file

    // 2 - Create external data source for the ingest control file
    azureSynapsePdao.createExternalDataSource(ingestFileLocation);

    // 3 - Build schema for dataset table

    // 4 - Create parquet files via external table

  }
}
