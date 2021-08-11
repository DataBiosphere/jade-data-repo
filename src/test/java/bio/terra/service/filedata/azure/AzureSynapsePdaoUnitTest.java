package bio.terra.service.filedata.azure;

import bio.terra.common.Column;
import bio.terra.common.SynapseColumn;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetTable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.stringtemplate.v4.ST;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AzureSynapsePdaoUnitTest {
  private static final Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoUnitTest.class);
  private static final String createTableSelectJSON =
      "SELECT <columns:{c|<c.name> <c.synapseDataType> <if(c.requiresCollate)>COLLATE Latin1_General_100_CI_AI_SC_UTF8<endif>}; separator=\",\n\">\nFROM\n"
          + "tableName;";

  @Test
  public void testStringTemplate() {
    List<String> columnNames =
        Arrays.asList("id", "age", "first_name", "last_name", "favorite_animals");
    TableDataType baseType = TableDataType.BOOLEAN;
    DatasetTable destinationTable =
        DatasetFixtures.generateDatasetTable("tableName", baseType, columnNames);

    List<SynapseColumn> formattedColumns =
        destinationTable.getColumns().stream()
            .map(Column::toSynapseColumn)
            .collect(Collectors.toList());

    ST template = new ST(createTableSelectJSON);
    template.add("columns", formattedColumns);
    logger.info(template.render());
  }
}
