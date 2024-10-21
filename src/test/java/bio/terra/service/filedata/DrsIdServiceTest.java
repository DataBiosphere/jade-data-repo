package bio.terra.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DrsIdServiceTest {

  private static final String HOSTNAME = "myhost.org";
  private static final String COMPACT_ID_PREFIX = "foo.0";

  private final ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();

  private DrsIdService drsIdService;

  @BeforeEach
  void setUp() throws Exception {
    applicationConfiguration.setDnsName(HOSTNAME);
    drsIdService = new DrsIdService(applicationConfiguration);
  }

  @Test
  void testV1DrsIds() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    assertThat(
        "v1 object id can be parsed",
        drsIdService.fromObjectId("v1_" + snapshotId + "_" + fileId),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString(), false)));
    assertThat(
        "v1 object id can be created",
        drsIdService.makeDrsId(new FSFile().fileId(fileId), snapshotId.toString()),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString(), false)));
    String drsUri = "drs://" + HOSTNAME + "/v1_" + snapshotId + "_" + fileId;
    assertThat(
        "v1 drs URI can be parsed",
        DrsIdService.fromUri(drsUri),
        equalTo(new DrsId(HOSTNAME, "v1", snapshotId.toString(), fileId.toString(), false)));
    assertThat(
        "v1 drs URI can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUri).toDrsUri(),
        equalTo(drsUri));
  }

  @Test
  void testV2DrsIds() {
    UUID fileId = UUID.randomUUID();
    assertThat(
        "v2 object id can be parsed",
        drsIdService.fromObjectId("v2_" + fileId),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString(), false)));
    assertThat(
        "v2 object id can be created",
        drsIdService.makeDrsId(new FSFile().fileId(fileId)),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString(), false)));
    String drsUri = "drs://" + HOSTNAME + "/v2_" + fileId;
    assertThat(
        "v2 drs URI can be parsed",
        DrsIdService.fromUri(drsUri),
        equalTo(new DrsId(HOSTNAME, "v2", null, fileId.toString(), false)));
    assertThat(
        "v2 drs URI can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUri).toDrsUri(),
        equalTo(drsUri));
  }

  @Test
  void testCompactIdDrsIds() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    String drsUriV1 = "drs://" + COMPACT_ID_PREFIX + ":v1_" + snapshotId + "_" + fileId;
    String drsUriV2 = "drs://" + COMPACT_ID_PREFIX + ":v2_" + fileId;
    assertThat(
        "v1 compact id can be parsed",
        DrsIdService.fromUri(drsUriV1),
        equalTo(
            new DrsId(COMPACT_ID_PREFIX, "v1", snapshotId.toString(), fileId.toString(), true)));
    assertThat(
        "v1 compact id can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUriV1).toDrsUri(),
        equalTo(drsUriV1));
    assertThat(
        "v2 compact id can be parsed",
        DrsIdService.fromUri(drsUriV2),
        equalTo(new DrsId(COMPACT_ID_PREFIX, "v2", null, fileId.toString(), true)));
    assertThat(
        "v2 compact id can be parsed and returns the same URI",
        DrsIdService.fromUri(drsUriV2).toDrsUri(),
        equalTo(drsUriV2));
  }

  @Test
  void testInvalidDrsIds() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    assertThrows(
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v3_" + snapshotId + "_" + fileId),
        "v3 object id cannot be parsed");

    assertThrows(
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v1_" + fileId),
        "badly formed v1 object id cannot be parsed - no snapshot");

    assertThrows(
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v1_" + snapshotId + "_" + fileId + "_foo"),
        "badly formed v1 object id cannot be parsed - extra field");

    assertThrows(
        InvalidDrsIdException.class,
        () -> drsIdService.fromObjectId("v2_" + snapshotId + "_" + fileId),
        "badly formed v2 object id cannot be parsed - has a snapshot");

    assertThrows(
        InvalidDrsIdException.class,
        () -> DrsIdService.fromUri("https://notadrsurl.com"),
        "badly formed uri - invalid protocol");

    assertThrows(
        InvalidDrsIdException.class,
        () -> DrsIdService.fromUri("drs://" + HOSTNAME),
        "badly formed uri - just a host");
  }

  @Test
  void testDrsObjectIdValidation() {
    UUID snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();

    assertTrue(drsIdService.isValidObjectId("v1_" + snapshotId + "_" + fileId), "v1 id is valid");
    assertTrue(drsIdService.isValidObjectId("v2_" + fileId), "v2 id is valid");
    assertFalse(drsIdService.isValidObjectId("v3_foo"), "invalid id is invalid");
  }
}
