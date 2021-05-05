package bio.terra.common;

import bio.terra.model.GoogleRegion;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.List;

public final class StorageRegions {

    private StorageRegions() {}

    public static final List<String> SUPPORTED_GOOGLE_REGIONS =
        Arrays.stream(GoogleRegion.values())
            .map(GoogleRegion::toString)
            .collect(Collectors.toUnmodifiableList());
}
