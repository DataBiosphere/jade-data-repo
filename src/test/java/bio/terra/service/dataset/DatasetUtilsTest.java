package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DatasetUtilsTest {

  @Test
  void generateAuxTableNameTest() {
    DatasetTable testTable = new DatasetTable().name("my_cool_table");
    String auxName1 = DatasetUtils.generateAuxTableName(testTable, "test");
    String auxName2 = DatasetUtils.generateAuxTableName(testTable, "test");

    for (String name : Arrays.asList(auxName1, auxName2)) {
      assertThat(
          name,
          allOf(
              startsWith(PdaoConstant.PDAO_PREFIX),
              containsString(testTable.getName()),
              containsString("test"),
              not(containsString("-"))));
    }

    assertNotEquals(auxName1, auxName2);
  }

  @Test
  void convertDatasetTest() {
    UUID fakeProfileId = UUID.randomUUID();
    DatasetRequestModel datasetRequest =
        TestUtils.loadObject("ingest-test-dataset.json", DatasetRequestModel.class)
            .defaultProfileId(fakeProfileId)
            .cloudPlatform(CloudPlatform.GCP);
    Dataset convertedDataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    for (DatasetTable table : convertedDataset.getTables()) {
      assertNotNull(table.getRawTableName());
      assertNotNull(table.getSoftDeleteTableName());
    }
  }
}
