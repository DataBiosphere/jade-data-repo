package bio.terra.service.journal;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamResourceType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class JournalService {
  private static final Logger logger = LoggerFactory.getLogger(JournalService.class);
  private final ObjectMapper objectMapper;
  private final JournalDao journalDao;

  public JournalService(ObjectMapper objectMapper, JournalDao journalDao) {
    this.objectMapper = objectMapper;
    this.journalDao = journalDao;
  }

  /**
   * Record a journal entry that indicates a resource was created
   *
   * @param user The user performing the action being journaled
   * @param resourceKey The UUID of the resource (domain object) the journal entry pertains to
   * @param resourceType The {@code IamResourceType} that maps to the domain object the journal
   *     entry pertains to
   * @param note An optional message describing the event
   * @param changeMap An optional map of changes to store with the event
   * @return UUID of the entry created.
   */
  public UUID recordCreate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID resourceKey,
      @NotNull IamResourceType resourceType,
      String note,
      Map<?, ?> changeMap) {
    return record(EntryType.CREATE, user, resourceKey, resourceType, note, changeMap, false);
  }

  /**
   * Record a journal entry that indicates a resource was created and optionally deletes matching
   * journal entries that may have existed. Billing profiles are an example of a domain object that
   * supports UUID reuse.
   *
   * @param user The user performing the action being journaled
   * @param resourceKey The UUID of the domain object the journal entry pertains to
   * @param resourceType The {@code IamResourceType} that maps to the domain object the journal
   *     entry pertains to
   * @param note An optional message describing the event
   * @param changeMap An optional map of changes to store with the event
   * @param clearHistory A boolean to indicate that any prior journal entries should be removed on
   *     this create.
   * @return UUID of the entry created.
   */
  public UUID recordCreate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID resourceKey,
      @NotNull IamResourceType resourceType,
      String note,
      Map<?, ?> changeMap,
      boolean clearHistory) {
    return record(EntryType.CREATE, user, resourceKey, resourceType, note, changeMap, clearHistory);
  }

  /**
   * Record a journal entry that indicates a resource was updated or had an action performed that
   * was worth noting.
   *
   * @param user The user performing the action being journaled
   * @param resourceKey The UUID of the domain object the journal entry pertains to
   * @param resourceType The {@code IamResourceType} that maps to the domain object the journal
   *     entry pertains to
   * @param note An optional message describing the event
   * @param changeMap An optional map of changes to store with the event
   * @return UUID of the entry created.
   */
  public UUID recordUpdate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID resourceKey,
      @NotNull IamResourceType resourceType,
      String note,
      Map<?, ?> changeMap) {
    return record(EntryType.UPDATE, user, resourceKey, resourceType, note, changeMap, false);
  }

  /**
   * Record a journal entry that indicates a resource was deleted.
   *
   * @param user The user performing the action being journaled
   * @param resourceKey The UUID of the domain object the journal entry pertains to
   * @param resourceType The {@code IamResourceType} that maps to the domain object the journal
   *     entry pertains to
   * @param note An optional message describing the event
   * @param changeMap An optional map of changes to store with the event
   * @return UUID of the entry created.
   */
  public UUID recordDelete(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID resourceKey,
      @NotNull IamResourceType resourceType,
      String note,
      Map<?, ?> changeMap) {
    return record(EntryType.DELETE, user, resourceKey, resourceType, note, changeMap, false);
  }

  /**
   * Return an ordered list of {@code JournalEntryModel}s sorted descending by when they were
   * created
   *
   * @param resourceKey The domain object key of the resource
   * @param resourceType The {@code IamResourceType} that maps to the domain object the journal
   *     entry pertains to
   * @param offset The zero based location to begin returning the list of Journal Entries
   * @param limit The limit of entries to be returned
   * @return {@code List<JournalEntryModel>}
   */
  public List<JournalEntryModel> getJournalEntries(
      UUID resourceKey, IamResourceType resourceType, long offset, int limit) {
    return journalDao.retrieveEntriesByIdAndType(resourceKey, resourceType, offset, limit);
  }

  private UUID record(
      EntryType entryType,
      AuthenticatedUserRequest user,
      UUID key,
      IamResourceType resourceType,
      String note,
      Map<?, ?> changeMap,
      boolean clearHistory) {
    StackWalker.StackFrame frame = getCallerFrame(3);
    Map<?, ?> nonNullValuesMap = null;
    String mapJson = null;
    if (changeMap != null) {
      nonNullValuesMap = filterNullValuesFromMap(changeMap);
    }
    try {
      if (nonNullValuesMap != null && !nonNullValuesMap.isEmpty()) {
        mapJson = objectMapper.writeValueAsString(nonNullValuesMap);
      }
    } catch (JsonProcessingException e) {
      logger.error(
          "Error encountered when creating a journal entry {} {} {} {} {} {} {} {} {}",
          entryType,
          user,
          key,
          resourceType,
          frame.getClassName(),
          frame.getMethodName(),
          note,
          nonNullValuesMap,
          e);
    }
    if (clearHistory) {
      journalDao.deleteJournalEntries(key, resourceType);
    }
    return journalDao.create(
        entryType,
        user.getEmail(),
        key,
        resourceType,
        frame.getClassName(),
        frame.getMethodName(),
        note,
        mapJson);
  }

  /**
   * Remove an entry from the journal.
   *
   * @param idToRemove The journal entry ID to remove.
   */
  public void removeJournalEntry(@NotNull UUID idToRemove) {
    journalDao.deleteJournalEntryById(idToRemove);
  }

  @VisibleForTesting
  public static StackWalker.StackFrame getCallerFrame(long n) {
    StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    return walker.walk(stream1 -> stream1.skip(n).findFirst().orElse(null));
  }

  private static <K, V> Map<K, V> filterNullValuesFromMap(@NotNull Map<K, V> changeMap) {
    return changeMap.entrySet().stream()
        .filter(entry -> entry.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public enum EntryType {
    CREATE("CREATE"),
    UPDATE("UPDATE"),
    DELETE("DELETE");

    public final String label;

    EntryType(String label) {
      this.label = label;
    }
  }
}
