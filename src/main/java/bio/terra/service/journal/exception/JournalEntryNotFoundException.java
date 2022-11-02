package bio.terra.service.journal.exception;

import bio.terra.common.exception.NotFoundException;

public class JournalEntryNotFoundException extends NotFoundException {
  public JournalEntryNotFoundException(String message) {
    super(message);
  }
}
