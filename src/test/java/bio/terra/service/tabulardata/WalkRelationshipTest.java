package bio.terra.service.tabulardata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetSpecification;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class WalkRelationshipTest {
  private final AssetUtils assetUtils = new AssetUtils(new JsonLoader(new ObjectMapper()));

  private WalkRelationship walkRelationship;

  @BeforeEach
  void setup() throws IOException {
    AssetSpecification assetSpecification = assetUtils.buildTestAssetSpec();
    walkRelationship = assetUtils.buildExampleWalkRelationship(assetSpecification);
  }

  @Test
  void visitRelationship() {
    String originalFromTable = walkRelationship.getFromTableName();
    boolean firstVisit = walkRelationship.visitRelationship(walkRelationship.getFromTableId());
    assertThat("First visit to relationship with valid start table should return true", firstVisit);
    assertThat(
        "Table designated as the 'FROM' table should remain constant since it maps to the startTableId",
        originalFromTable,
        equalTo(walkRelationship.getFromTableName()));

    boolean secondVisit = walkRelationship.visitRelationship(walkRelationship.getToTableId());

    assertThat(
        "visitRelationship should return false since we have already visited this relationship.",
        not(secondVisit));
  }

  @Test
  void visitRelationshipSwapWalkDirection() {
    String originalFromTable = walkRelationship.getFromTableName();
    boolean firstVisit = walkRelationship.visitRelationship(walkRelationship.getToTableId());
    assertThat("First visit to relationship with valid start table should return true", firstVisit);
    assertThat(
        """
            Given a start table id that maps to the 'TO' table, the 'FROM' & 'TO' tables swap.
            The original 'FROM' table no longer equals the current 'FROM' table.
            """,
        originalFromTable,
        not(walkRelationship.getFromTableName()));

    boolean swappedStartTableVisit =
        walkRelationship.visitRelationship(walkRelationship.getFromTableId());

    assertThat(
        "The relationship was already visited, so visitRelationship should return false, even though the start table Id is different",
        false,
        is(swappedStartTableVisit));
  }

  @Test
  void visitRelationshipNotConnectedStartTable() {
    assertThat(
        "Visiting relationship with start table not defined in relationship should return false",
        false,
        is(walkRelationship.visitRelationship(UUID.randomUUID())));
  }
}
