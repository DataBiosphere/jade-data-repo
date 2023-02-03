package bio.terra.service.filedata;

import bio.terra.service.dataset.Dataset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class FileIdService {
  private static final String HASH_SEPARATOR = "#";

  public UUID calculateFileId(Dataset dataset, FSItem fsItem) {
    return calculateFileId(dataset.hasPredictableFileIds(), fsItem);
  }

  public UUID calculateFileId(boolean hasPredictableFileIds, FSItem fsItem) {
    if (hasPredictableFileIds) {
      return UUID.nameUUIDFromBytes(createHashableContent(fsItem).getBytes(StandardCharsets.UTF_8));
    } else {
      return UUID.randomUUID();
    }
  }

  private String createHashableContent(FSItem fsItem) {
    return String.join(
        HASH_SEPARATOR,
        List.of(
            fsItem.getPath(),
            Objects.requireNonNullElse(fsItem.getChecksumMd5(), ""),
            String.valueOf(fsItem.getSize())));
  }
}
