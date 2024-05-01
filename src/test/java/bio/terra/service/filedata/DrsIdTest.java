package bio.terra.service.filedata;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DrsIdTest {

  @Test
  void testDrsIdV1() {
    DrsId drsId =
        DrsId.builder()
            .dnsname("dns")
            .version("v1")
            .snapshotId("snapshot")
            .fsObjectId("file")
            .build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("v1"));
    assertThat(
        "drsid constructor succeeds - snapshotId", drsId.getSnapshotId(), equalTo("snapshot"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns/v1_snapshot_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("v1_snapshot_file"));
  }

  @Test
  void testDrsIdV2() {
    DrsId drsId = DrsId.builder().dnsname("dns").version("v2").fsObjectId("file").build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("v2"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns/v2_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("v2_file"));
  }

  @Test
  void testCompactDrsURI() {
    DrsId drsId =
        DrsId.builder().dnsname("dns").version("v2").fsObjectId("file").compactId(true).build();

    assertThat("drsid constructor succeeds - dnsname", drsId.getDnsname(), equalTo("dns"));
    assertThat("drsid constructor succeeds - version", drsId.getVersion(), equalTo("v2"));
    assertThat("drsid constructor succeeds - fileId", drsId.getFsObjectId(), equalTo("file"));
    assertThat("drsid toDrsUri works", drsId.toDrsUri(), equalTo("drs://dns:v2_file"));
    assertThat("drsid toObjectId works", drsId.toDrsObjectId(), equalTo("v2_file"));
  }

  @Test
  void testBadUri() {
    DrsId drsId =
        DrsId.builder().dnsname("{[]}").version("vv").snapshotId("{[]}").fsObjectId("file").build();

    assertThrows(InvalidDrsIdException.class, drsId::toDrsUri);
  }
}
