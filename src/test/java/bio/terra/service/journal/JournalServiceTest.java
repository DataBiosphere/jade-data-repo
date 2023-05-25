package bio.terra.service.journal;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class JournalServiceTest {

  private JournalService journalService;
  @Mock private ObjectMapper objectMapper;
  @Mock private JournalDao journalDao;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final IamResourceType RESOURCE_TYPE = IamResourceType.DATASET;
  private static final String NOTE = "journal entry note";
  private static final String CLASS_NAME = "bio.terra.service.journal.JournalServiceTest";
  private static final String CHANGE_MAP_STRING = "stringified-change-map";

  @BeforeEach
  void setUp() {
    journalService = new JournalService(objectMapper, journalDao);
  }

  private static Stream<Arguments> changeMapArguments() {
    Map<String, Object> mapWithNullEntry = new HashMap<>();
    mapWithNullEntry.put("NULL", null);

    Map<String, Object> mapWithNonNullEntry = Map.of("K", "V");
    Map<String, Object> mapWithNullAndNonNullEntries = new HashMap<>(mapWithNonNullEntry);
    mapWithNullEntry.put("NULL", null);

    return Stream.of(
        arguments(null, null),
        arguments(mapWithNullEntry, Map.of()),
        arguments(mapWithNullAndNonNullEntries, mapWithNonNullEntry));
  }

  @ParameterizedTest
  @MethodSource("changeMapArguments")
  void testRecordCreate(Map<String, Object> changeMap, Map<String, Object> filteredMap)
      throws JsonProcessingException {
    boolean changeMapHasEntries = filteredMap != null && !filteredMap.isEmpty();
    String expectedChangeMapString = null;
    if (changeMapHasEntries) {
      expectedChangeMapString = CHANGE_MAP_STRING;
      when(objectMapper.writeValueAsString(filteredMap)).thenReturn(CHANGE_MAP_STRING);
    }

    journalService.recordCreate(TEST_USER, RESOURCE_ID, RESOURCE_TYPE, NOTE, changeMap);

    if (changeMapHasEntries) {
      verify(objectMapper).writeValueAsString(filteredMap);
    } else {
      verifyNoInteractions(objectMapper);
    }
    verify(journalDao, never()).deleteJournalEntries(RESOURCE_ID, RESOURCE_TYPE);
    verify(journalDao)
        .create(
            JournalService.EntryType.CREATE,
            TEST_USER.getEmail(),
            RESOURCE_ID,
            RESOURCE_TYPE,
            CLASS_NAME,
            "testRecordCreate",
            NOTE,
            expectedChangeMapString);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRecordCreateClearHistory(boolean clearHistory) {
    journalService.recordCreate(TEST_USER, RESOURCE_ID, RESOURCE_TYPE, NOTE, null, clearHistory);

    verifyNoInteractions(objectMapper);
    var clearHistoryVerificationMode = (clearHistory) ? times(1) : never();
    verify(journalDao, clearHistoryVerificationMode)
        .deleteJournalEntries(RESOURCE_ID, RESOURCE_TYPE);
    verify(journalDao)
        .create(
            JournalService.EntryType.CREATE,
            TEST_USER.getEmail(),
            RESOURCE_ID,
            RESOURCE_TYPE,
            CLASS_NAME,
            "testRecordCreateClearHistory",
            NOTE,
            null);
  }

  @ParameterizedTest
  @MethodSource("changeMapArguments")
  void testRecordUpdate(Map<String, Object> changeMap, Map<String, Object> filteredMap)
      throws JsonProcessingException {
    boolean changeMapHasEntries = filteredMap != null && !filteredMap.isEmpty();
    String expectedChangeMapString = null;
    if (changeMapHasEntries) {
      expectedChangeMapString = CHANGE_MAP_STRING;
      when(objectMapper.writeValueAsString(filteredMap)).thenReturn(CHANGE_MAP_STRING);
    }

    journalService.recordUpdate(TEST_USER, RESOURCE_ID, RESOURCE_TYPE, NOTE, changeMap);

    if (changeMapHasEntries) {
      verify(objectMapper).writeValueAsString(filteredMap);
    } else {
      verifyNoInteractions(objectMapper);
    }
    verify(journalDao, never()).deleteJournalEntries(RESOURCE_ID, RESOURCE_TYPE);
    verify(journalDao)
        .create(
            JournalService.EntryType.UPDATE,
            TEST_USER.getEmail(),
            RESOURCE_ID,
            RESOURCE_TYPE,
            CLASS_NAME,
            "testRecordUpdate",
            NOTE,
            expectedChangeMapString);
  }

  @ParameterizedTest
  @MethodSource("changeMapArguments")
  void testRecordDelete(Map<String, Object> changeMap, Map<String, Object> filteredMap)
      throws JsonProcessingException {
    boolean changeMapHasEntries = filteredMap != null && !filteredMap.isEmpty();
    String expectedChangeMapString = null;
    if (changeMapHasEntries) {
      expectedChangeMapString = CHANGE_MAP_STRING;
      when(objectMapper.writeValueAsString(filteredMap)).thenReturn(CHANGE_MAP_STRING);
    }

    journalService.recordDelete(TEST_USER, RESOURCE_ID, RESOURCE_TYPE, NOTE, changeMap);

    if (changeMapHasEntries) {
      verify(objectMapper).writeValueAsString(filteredMap);
    } else {
      verifyNoInteractions(objectMapper);
    }
    verify(journalDao, never()).deleteJournalEntries(RESOURCE_ID, RESOURCE_TYPE);
    verify(journalDao)
        .create(
            JournalService.EntryType.DELETE,
            TEST_USER.getEmail(),
            RESOURCE_ID,
            RESOURCE_TYPE,
            CLASS_NAME,
            "testRecordDelete",
            NOTE,
            expectedChangeMapString);
  }

  @Test
  void testGetJournalEntries() {
    journalService.getJournalEntries(RESOURCE_ID, RESOURCE_TYPE, 0, 10);
    verify(journalDao).retrieveEntriesByIdAndType(RESOURCE_ID, RESOURCE_TYPE, 0, 10);
  }

  @Test
  void testRemoveJournalEntriesByFlightId() {
    String flightId = "a-flight-id";
    journalService.removeJournalEntriesByFlightId(flightId);
    verify(journalDao).deleteJournalEntriesByFlightId(flightId);
  }
}
