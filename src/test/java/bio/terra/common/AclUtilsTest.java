package bio.terra.common;

import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.BigQueryAclExhaustionException;
import com.google.cloud.bigquery.BigQueryException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class AclUtilsTest {

  @Test
  void aclUpdateRetry() {
    assertThrows(
        BigQueryAclExhaustionException.class,
        () ->
            AclUtils.aclUpdateRetry(
                () -> {
                  throw new BigQueryException(
                      0,
                      "Too many authorized entities in this dataset. The maximum number of authorized views, routines, and datasets combined is 2500.");
                }));
  }
}
