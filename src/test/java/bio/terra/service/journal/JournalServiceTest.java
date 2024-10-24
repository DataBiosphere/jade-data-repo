package bio.terra.service.journal;

import static bio.terra.service.journal.JournalService.getCallerFrame;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.job.JobMapKeys;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class JournalServiceTest {

  private static final AuthenticatedUserRequest TEST_USER1 =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final AuthenticatedUserRequest TEST_USER2 =
      AuthenticatedUserRequest.builder()
          .setSubjectId("Dataset2Unit")
          .setEmail("dataset2@unit.com")
          .setToken("token2")
          .build();

  @Autowired private JournalService journalService;

  @Test
  void journalCreateTest_EmptyMap() {
    UUID key = UUID.randomUUID();
    Map<String, Object> emptyMap = new LinkedHashMap<>();
    String note = "create note1";
    journalService.recordCreate(TEST_USER1, key, IamResourceType.DATASET, note, emptyMap);
    validateEntries(
        1, key, JournalService.EntryType.CREATE, IamResourceType.DATASET, TEST_USER1, note, null);
  }

  @Test
  void journalCreateTest_SameKeyDifferentResourceType() {
    UUID key = UUID.randomUUID();
    Map<String, Object> emptyMap = new LinkedHashMap<>();
    String note = "create note1";
    journalService.recordCreate(TEST_USER1, key, IamResourceType.DATASET, note, emptyMap);
    validateEntries(
        1, key, JournalService.EntryType.CREATE, IamResourceType.DATASET, TEST_USER1, note, null);
    journalService.recordCreate(TEST_USER1, key, IamResourceType.DATASNAPSHOT, note, emptyMap);
    validateEntries(
        1,
        key,
        JournalService.EntryType.CREATE,
        IamResourceType.DATASNAPSHOT,
        TEST_USER1,
        note,
        null);
  }

  @Test
  void journalGetResults_InvalidOffset() {
    UUID key = UUID.randomUUID();
    DataIntegrityViolationException ex =
        assertThrows(
            DataIntegrityViolationException.class,
            () -> journalService.getJournalEntries(key, IamResourceType.DATASNAPSHOT, -1, 0),
            "invalid offset throws.");
    assertThat(ex.getMessage(), containsString("OFFSET must not be negative"));
  }

  @Test
  void journalGetResults_EmptyResults() {
    UUID key = UUID.randomUUID();
    List<JournalEntryModel> emptyResults =
        journalService.getJournalEntries(key, IamResourceType.DATASNAPSHOT, 0, 0);
    assertThat("results should be empty", emptyResults, empty());
  }

  @Test
  void journalCreateTest_SimpleEntries() {
    Map<String, Object> simpleMap = new LinkedHashMap<>();
    simpleMap.put("NULL", null);
    simpleMap.put("KEY", "VALUE");
    String note1 = "update note1";
    String note2 = "delete note1";
    Map<String, Object> simpleMap2 = new LinkedHashMap<>();
    simpleMap2.put("KEY2", "VALUE2");
    UUID key = UUID.randomUUID();
    journalService.recordCreate(TEST_USER1, key, IamResourceType.DATASET, null, simpleMap);
    validateEntries(
        1,
        key,
        JournalService.EntryType.CREATE,
        IamResourceType.DATASET,
        TEST_USER1,
        null,
        simpleMap.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    journalService.recordUpdate(TEST_USER1, key, IamResourceType.DATASET, note1, simpleMap2);
    validateEntries(
        2,
        key,
        JournalService.EntryType.UPDATE,
        IamResourceType.DATASET,
        TEST_USER1,
        note1,
        simpleMap2);

    journalService.recordDelete(TEST_USER1, key, IamResourceType.DATASET, note2, null);
    validateEntries(
        3, key, JournalService.EntryType.DELETE, IamResourceType.DATASET, TEST_USER1, note2, null);

    List<JournalEntryModel> queryResult =
        journalService.getJournalEntries(key, IamResourceType.DATASET, 1, 1);
    assertThat("The query result should only have one entry", queryResult, hasSize(1));
    assertThat(
        "The query result should be the update entry",
        queryResult.get(0).getEntryType(),
        equalTo(JournalEntryModel.EntryTypeEnum.UPDATE));

    journalService.recordCreate(TEST_USER2, key, IamResourceType.DATASET, null, simpleMap, true);
    validateEntries(
        1,
        key,
        JournalService.EntryType.CREATE,
        IamResourceType.DATASET,
        TEST_USER2,
        null,
        simpleMap.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Test
  void journal_DeleteEntriesByFlightIdTest() {
    UUID datasetId = UUID.randomUUID();
    String flightId = UUID.randomUUID().toString();
    Map<String, Object> changeMap = new LinkedHashMap<>();
    changeMap.put("FLIGHT_ID", flightId);
    changeMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "foo");
    String note = "create note1";
    journalService.recordUpdate(TEST_USER1, datasetId, IamResourceType.DATASET, note, changeMap);
    validateEntries(
        1,
        datasetId,
        JournalService.EntryType.UPDATE,
        IamResourceType.DATASET,
        TEST_USER1,
        note,
        changeMap);

    UUID snapshotId = UUID.randomUUID();
    journalService.recordCreate(
        TEST_USER1, snapshotId, IamResourceType.DATASNAPSHOT, note, changeMap);
    validateEntries(
        1,
        snapshotId,
        JournalService.EntryType.CREATE,
        IamResourceType.DATASNAPSHOT,
        TEST_USER1,
        note,
        changeMap);

    UUID profileId = UUID.randomUUID();
    String flightId2 = UUID.randomUUID().toString();
    Map<String, Object> changeMap2 = new LinkedHashMap<>();
    changeMap2.put(JobMapKeys.DESCRIPTION.getKeyName(), "bar");
    changeMap2.put("FLIGHT_ID", flightId2);
    String note2 = "create note2";
    journalService.recordCreate(
        TEST_USER1, profileId, IamResourceType.SPEND_PROFILE, note2, changeMap2, true);
    validateEntries(
        1,
        profileId,
        JournalService.EntryType.CREATE,
        IamResourceType.SPEND_PROFILE,
        TEST_USER1,
        note2,
        changeMap2);

    assertThat(
        "there should be an entry for this dataset create.",
        journalService.getJournalEntries(datasetId, IamResourceType.DATASET, 0, 10),
        hasSize(1));
    assertThat(
        "there should be an entry for this snapshot create.",
        journalService.getJournalEntries(snapshotId, IamResourceType.DATASNAPSHOT, 0, 10),
        hasSize(1));
    assertThat(
        "there should be an entry for the profile create.",
        journalService.getJournalEntries(profileId, IamResourceType.SPEND_PROFILE, 0, 10),
        hasSize(1));

    journalService.removeJournalEntriesByFlightId(flightId);
    assertThat(
        "the dataset journal entry should have been removed.",
        journalService.getJournalEntries(datasetId, IamResourceType.DATASET, 0, 10),
        empty());
    assertThat(
        "the snapshot journal entry should have been removed.",
        journalService.getJournalEntries(snapshotId, IamResourceType.DATASNAPSHOT, 0, 10),
        empty());
    assertThat(
        "the profile entry should not have been removed.",
        journalService.getJournalEntries(profileId, IamResourceType.SPEND_PROFILE, 0, 10),
        hasSize(1));

    // test removing journal entries for the same flightId twice (which would happen if a
    // flight with multiple journal steps fails)
    journalService.removeJournalEntriesByFlightId(flightId);
  }

  /**
   * Validate the count of journal entries for the specified key and resourceType, and validate the
   * contents of the first (most recent) entry.
   */
  private void validateEntries(
      int expectedCount,
      UUID key,
      JournalService.EntryType entryType,
      IamResourceType resourceType,
      AuthenticatedUserRequest user,
      String note,
      Map<String, Object> entryMap) {
    List<JournalEntryModel> results = journalService.getJournalEntries(key, resourceType, 0, 100);
    assertThat(
        "the number of entries matches the expected count.", results, hasSize(expectedCount));
    JournalEntryModel entryUnderTest = results.get(0);
    assertThat(
        "the journal resource key matches the UUID", entryUnderTest.getResourceKey(), equalTo(key));
    assertThat(
        "the journal entry resource type is the expected value",
        IamResourceType.fromEnum(entryUnderTest.getResourceType()).toString(),
        equalToIgnoringCase(resourceType.toString()));
    assertThat(
        "the journal entry type is correct",
        entryUnderTest.getEntryType().toString(),
        equalToIgnoringCase(entryType.toString()));
    assertThat(
        "the journal entry user is correct", entryUnderTest.getUser(), equalTo(user.getEmail()));
    assertThat(
        "the journal entry mutations element is the expected value",
        entryUnderTest.getMutation(),
        equalTo(entryMap));
    assertThat(
        "the journal entry class name is from this class.",
        entryUnderTest.getClassName(),
        equalTo(this.getClass().getCanonicalName()));
    assertThat(
        "the journal entry method name is from the test method.",
        entryUnderTest.getMethodName(),
        equalTo(getCallerFrame(2).getMethodName()));
    assertThat(
        "the journal entry note is properly recorded.", entryUnderTest.getNote(), equalTo(note));
  }
}
