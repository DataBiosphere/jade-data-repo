package bio.terra.service.dataset;

import java.util.UUID;

public class Storage {
    private UUID id;
    private String region;

    public Storage id(UUID id) {
        this.id = id;
        return this;
    }

    public UUID getId() {
        return id;
    }

    public Storage region(String region) {
        this.region = region;
        return this;
    }

    public String getRegion() {
        return region;
    }

}
