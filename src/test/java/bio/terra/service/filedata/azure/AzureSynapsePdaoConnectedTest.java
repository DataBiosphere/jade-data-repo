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
    boolean success = azureSynapsePdao.runAQuery("Select * FROM ourCoolTable;");
    logger.info("After query. {}", success);
  }
}
