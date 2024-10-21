package bio.terra.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertNull;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.filedata.DrsDao.DrsAlias;
import bio.terra.service.filedata.DrsDao.DrsAliasSpec;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class DrsDaoTest {

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @Autowired private DrsIdService drsIdService;
  @Autowired private DrsDao drsDao;

  @Test
  void testInsertAndRetrieveAndDeleteDrsAlias() {
    String flightId = "fooflight";
    assertThat(
        "1 row was written",
        drsDao.insertDrsAlias(
            List.of(new DrsAliasSpec("foo", drsIdService.fromObjectId("v1_123_456"))),
            flightId,
            TEST_USER),
        equalTo(1L));
    assertThat(
        "1 row can be read",
        drsDao.retrieveDrsAliasByAlias("foo"),
        samePropertyValuesAs(
            new DrsAlias(
                UUID.randomUUID(),
                "foo",
                drsIdService.fromObjectId("v1_123_456"),
                Instant.now(),
                TEST_USER.getEmail(),
                flightId),
            "id",
            "createdDate"));
    assertThat("1 row can be deleted", drsDao.deleteDrsAliasByFlight(flightId), equalTo(1L));
    assertNull("no rows left", drsDao.retrieveDrsAliasByAlias("foo"));
  }
}
