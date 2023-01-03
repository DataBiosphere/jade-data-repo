package bio.terra.service.tabulardata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.DatasetTable;
import java.io.IOException;
import java.util.UUID;
import org.junit.Before;
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
public class WalkRelationshipTest {
  @Autowired AssetUtils assetUtils;

  WalkRelationship walkRelationship;

  @Before
  public void setup() throws IOException {
    AssetSpecification assetSpecification = assetUtils.buildTestAssetSpec();
    DatasetTable participantTable =
        assetSpecification.getAssetTableByName("participant").getTable();
    DatasetTable sampleTable = assetSpecification.getAssetTableByName("sample").getTable();
    // define relationships
    AssetRelationship sampleParticipantRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(participantTable.getColumnByName("id"))
                    .fromTable(participantTable)
                    .toColumn(sampleTable.getColumnByName("participant_ids"))
                    .toTable(sampleTable)
                    .name("participant_sample_relationship"));
    walkRelationship = WalkRelationship.ofAssetRelationship(sampleParticipantRelationship);
  }

  @Test
  public void visitRelationship() {
    String originalFromTable = walkRelationship.getFromTableName();
    Boolean firstVisit = walkRelationship.visitRelationship(walkRelationship.getFromTableId());
    assertThat("First visit to relationship with valid start table should return true", firstVisit);
    assertThat(
        "Table designated as the 'FROM' table should remain constant since it maps to the startTableId",
        originalFromTable,
        equalTo(walkRelationship.getFromTableName()));

    Boolean secondVisit = walkRelationship.visitRelationship(walkRelationship.getToTableId());

    assertThat(
        "visitRelationship should return false since we have already visited this relationship.",
        not(secondVisit));
  }

  @Test
  public void visitRelationshipSwapWalkDirection() {
    String originalFromTable = walkRelationship.getFromTableName();
    Boolean firstVisit = walkRelationship.visitRelationship(walkRelationship.getToTableId());
    assertThat("First visit to relationship with valid start table should return true", firstVisit);
    assertThat(
        """
            Given a start table id that maps to the 'TO' table, the 'FROM' & 'TO' tables swap.
            The original 'FROM' table no longer equals the current 'FROM' table.
            """,
        originalFromTable,
        not(walkRelationship.getFromTableName()));

    Boolean swappedStartTableVisit =
        walkRelationship.visitRelationship(walkRelationship.getFromTableId());

    assertThat(
        "The relationship was already visited, so visitRelationship should return false, even though the start table Id is different",
        false,
        is(swappedStartTableVisit));
  }

  @Test
  public void visitRelationshipNotConnectedStartTable() {
    assertThat(
        "Visiting relationship with start table not defined in relationship should return false",
        false,
        is(walkRelationship.visitRelationship(UUID.randomUUID())));
  }
}
