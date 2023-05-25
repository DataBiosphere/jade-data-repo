package bio.terra.service.journal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService.EntryType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@AutoConfigureMockMvc
@EmbeddedDatabaseTest
@SpringBootTest
@Tag("bio.terra.common.category.Unit")
public final class JournalDaoTest {

  @Autowired private JournalDao journalDao;
  @Autowired private ObjectMapper objectMapper;

  private List<UUID> journaledDatasetIds;
  private List<UUID> journaledSnapshotIds;

  private static final String USER1_EMAIL = "user1@gmail.com";
  private static final String USER2_EMAIL = "user2@gmail.com";
  private static final String CLASS_NAME = "class_name";
  private static final String METHOD_NAME = "method_name";

  @BeforeEach
  void setUp() {
    journaledDatasetIds = new ArrayList<>();
    journaledSnapshotIds = new ArrayList<>();
  }

  @AfterEach
  void tearDown() {
    for (var datasetId : journaledDatasetIds) {
      journalDao.deleteJournalEntries(datasetId, IamResourceType.DATASET);
    }
    for (var snapshotId : journaledSnapshotIds) {
      journalDao.deleteJournalEntries(snapshotId, IamResourceType.DATASNAPSHOT);
    }
  }

  @ParameterizedTest
  @EnumSource
  void testCreateJournalEntriesWithSameResourceKeyAndDifferentResourceTypes(EntryType entryType) {
    UUID resourceId = UUID.randomUUID();

    String datasetNote = "dataset note";
    journalDao.create(
        entryType,
        USER1_EMAIL,
        resourceId,
        IamResourceType.DATASET,
        CLASS_NAME,
        METHOD_NAME,
        datasetNote,
        null);
    journaledDatasetIds.add(resourceId);

    String snapshotNote = "snapshot note";
    journalDao.create(
        entryType,
        USER2_EMAIL,
        resourceId,
        IamResourceType.DATASNAPSHOT,
        CLASS_NAME,
        METHOD_NAME,
        snapshotNote,
        null);
    journaledSnapshotIds.add(resourceId);

    validateEntries(
        1, resourceId, entryType, IamResourceType.DATASET, USER1_EMAIL, datasetNote, null);
    validateEntries(
        1, resourceId, entryType, IamResourceType.DATASNAPSHOT, USER2_EMAIL, snapshotNote, null);
  }

  @Test
  void testGetResultsWithInvalidOffsetThrows() {
    DataIntegrityViolationException ex =
        assertThrows(
            DataIntegrityViolationException.class,
            () ->
                journalDao.retrieveEntriesByIdAndType(
                    UUID.randomUUID(), IamResourceType.DATASET, -1, 0),
            "invalid offset throws");
    assertThat(ex.getMessage(), containsString("OFFSET must not be negative"));
  }

  private Map<String, Object> mapWithFlightId(String flightId) {
    Map<String, Object> map = new HashMap<>();
    map.put("FLIGHT_ID", flightId);
    return map;
  }

  @Test
  void testCreateJournalEntriesWithMaps() throws JsonProcessingException {
    String flightId1 = "flightId1";
    Map<String, Object> flightMap1 = mapWithFlightId(flightId1);

    String flightId2 = "flightId2";
    Map<String, Object> flightMap2 = mapWithFlightId(flightId2);

    UUID datasetId = UUID.randomUUID();
    String datasetNote = "dataset note";

    UUID snapshotId = UUID.randomUUID();
    String snapshotNote = "snapshot note";

    // 1. Flight 1 writes a journal for dataset
    journalDao.create(
        EntryType.CREATE,
        USER1_EMAIL,
        datasetId,
        IamResourceType.DATASET,
        CLASS_NAME,
        METHOD_NAME,
        datasetNote,
        objectMapper.writeValueAsString(flightMap1));
    journaledDatasetIds.add(datasetId);
    validateEntries(
        1,
        datasetId,
        EntryType.CREATE,
        IamResourceType.DATASET,
        USER1_EMAIL,
        datasetNote,
        flightMap1);

    // 2. Flight 1 writes a journal for snapshot
    journalDao.create(
        EntryType.CREATE,
        USER1_EMAIL,
        snapshotId,
        IamResourceType.DATASNAPSHOT,
        CLASS_NAME,
        METHOD_NAME,
        snapshotNote,
        objectMapper.writeValueAsString(flightMap1));
    journaledSnapshotIds.add(snapshotId);
    validateEntries(
        1,
        snapshotId,
        EntryType.CREATE,
        IamResourceType.DATASNAPSHOT,
        USER1_EMAIL,
        snapshotNote,
        flightMap1);

    // 3. Flight 2 writes a journal for dataset
    journalDao.create(
        EntryType.UPDATE,
        USER2_EMAIL,
        datasetId,
        IamResourceType.DATASET,
        CLASS_NAME,
        METHOD_NAME,
        datasetNote,
        objectMapper.writeValueAsString(flightMap2));
    validateEntries(
        2,
        datasetId,
        EntryType.UPDATE,
        IamResourceType.DATASET,
        USER2_EMAIL,
        datasetNote,
        flightMap2);

    // 4. Remove journal entries for flight 1
    journalDao.deleteJournalEntriesByFlightId(flightId1);
    validateEntries(
        1,
        datasetId,
        EntryType.UPDATE,
        IamResourceType.DATASET,
        USER2_EMAIL,
        datasetNote,
        flightMap2);
    assertThat(
        "The snapshot journal entry from flight 1 was removed",
        journalDao.retrieveEntriesByIdAndType(snapshotId, IamResourceType.DATASNAPSHOT, 0, 10),
        empty());

    // 4. Repeating the removal of journal entries for flight 1 has no impact
    journalDao.deleteJournalEntriesByFlightId(flightId1);
    validateEntries(
        1,
        datasetId,
        EntryType.UPDATE,
        IamResourceType.DATASET,
        USER2_EMAIL,
        datasetNote,
        flightMap2);
    assertThat(
        "The snapshot journal entry from flight 1 was removed",
        journalDao.retrieveEntriesByIdAndType(snapshotId, IamResourceType.DATASNAPSHOT, 0, 10),
        empty());
  }

  /**
   * Validate the count of journal entries for the specified key and resourceType, and validate the
   * contents of the first (most recent) entry.
   */
  private void validateEntries(
      int expectedCount,
      UUID resourceId,
      EntryType entryType,
      IamResourceType resourceType,
      String userEmail,
      String note,
      Map<String, Object> entryMap) {
    List<JournalEntryModel> results =
        journalDao.retrieveEntriesByIdAndType(resourceId, resourceType, 0, 100);
    assertThat(
        "the number of entries matches the expected count.", results, hasSize(expectedCount));
    JournalEntryModel entryUnderTest = results.get(0);
    assertThat(
        "the journal resource key matches the UUID",
        entryUnderTest.getResourceKey(),
        equalTo(resourceId));
    assertThat(
        "the journal entry resource type is the expected value",
        IamResourceType.fromEnum(entryUnderTest.getResourceType()).toString(),
        equalToIgnoringCase(resourceType.toString()));
    assertThat(
        "the journal entry type is correct",
        entryUnderTest.getEntryType().toString(),
        equalToIgnoringCase(entryType.toString()));
    assertThat("the journal entry user is correct", entryUnderTest.getUser(), equalTo(userEmail));
    assertThat(
        "the journal entry mutations element is the expected value",
        entryUnderTest.getMutation(),
        equalTo(entryMap));
    assertThat(
        "the journal entry class name is the expected value",
        entryUnderTest.getClassName(),
        equalTo(CLASS_NAME));
    assertThat(
        "the journal entry method name is the expected value",
        entryUnderTest.getMethodName(),
        equalTo(METHOD_NAME));
    assertThat(
        "the journal entry note is properly recorded", entryUnderTest.getNote(), equalTo(note));
  }
}
