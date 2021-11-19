package bio.terra.service.dataset;

import bio.terra.common.PdaoConstant;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import java.util.Arrays;
import java.util.UUID;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetUtilsTest {

  @Autowired private JsonLoader jsonLoader;

  @Test
  public void generateAuxTableNameTest() {
    DatasetTable testTable = new DatasetTable().name("my_cool_table");
    String auxName1 = DatasetUtils.generateAuxTableName(testTable, "test");
    String auxName2 = DatasetUtils.generateAuxTableName(testTable, "test");

    for (String name : Arrays.asList(auxName1, auxName2)) {
      Assert.assertThat(name, Matchers.startsWith(PdaoConstant.PDAO_PREFIX));
      Assert.assertThat(name, Matchers.containsString(testTable.getName()));
      Assert.assertThat(name, Matchers.containsString("test"));
      Assert.assertThat(name, Matchers.not(Matchers.containsString("-")));
    }

    Assert.assertNotEquals(auxName1, auxName2);
  }

  @Test
  public void convertDatasetTest() throws Exception {
    UUID fakeProfileId = UUID.randomUUID();
    DatasetRequestModel datasetRequest =
        jsonLoader
            .loadObject("ingest-test-dataset.json", DatasetRequestModel.class)
            .defaultProfileId(fakeProfileId)
            .cloudPlatform(CloudPlatform.GCP);
    Dataset convertedDataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    for (DatasetTable table : convertedDataset.getTables()) {
      Assert.assertNotNull(table.getRawTableName());
      Assert.assertNotNull(table.getSoftDeleteTableName());
    }
  }
}
