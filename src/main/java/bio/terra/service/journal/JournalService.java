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

  public void journalCreate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID key,
      @NotNull IamResourceType resourceType,
      String note,
      Map<String, Object> changeMap) {
    journal(EntryType.CREATE, user, key, resourceType, note, changeMap, false);
  }

  public void journalCreate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID key,
      @NotNull IamResourceType resourceType,
      String note,
      Map<String, Object> changeMap,
      boolean clearHistory) {
    journal(EntryType.CREATE, user, key, resourceType, note, changeMap, clearHistory);
  }

  public void journalUpdate(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID key,
      @NotNull IamResourceType resourceType,
      String note,
      Map<String, Object> changeMap) {
    journal(EntryType.UPDATE, user, key, resourceType, note, changeMap, false);
  }

  public void journalDelete(
      @NotNull AuthenticatedUserRequest user,
      @NotNull UUID key,
      @NotNull IamResourceType resourceType,
      String note,
      Map<String, Object> changeMap) {
    journal(EntryType.DELETE, user, key, resourceType, note, changeMap, false);
  }

  public List<JournalEntryModel> getJournalEntries(
      UUID resourceKey, IamResourceType resourceType, long offset, int limit) {
    return journalDao.retrieveEntriesByIdAndType(resourceKey, resourceType, offset, limit);
  }

  private void journal(
      EntryType entryType,
      AuthenticatedUserRequest user,
      UUID key,
      IamResourceType resourceType,
      String note,
      Map<String, Object> changeMap,
      boolean clearHistory) {
    StackWalker.StackFrame frame = getCallerFrame(3);
    Map nonNullValuesMap = null;
    String mapJson = null;
    if (changeMap != null) {
      nonNullValuesMap =
          changeMap.entrySet().stream()
              .filter(entry -> entry.getValue() != null)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
    journalDao.create(
        entryType,
        user.getEmail(),
        key,
        resourceType,
        frame.getClassName(),
        frame.getMethodName(),
        note,
        mapJson);
  }

  @VisibleForTesting
  public static StackWalker.StackFrame getCallerFrame(long n) {
    StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    return walker.walk(stream1 -> stream1.skip(n).findFirst().orElse(null));
  }

  public enum EntryType {
    CREATE("CREATE"),
    UPDATE("UPDATE"),
    DELETE("DELETE");

    public final String label;

    private EntryType(String label) {
      this.label = label;
    }
  }
}
