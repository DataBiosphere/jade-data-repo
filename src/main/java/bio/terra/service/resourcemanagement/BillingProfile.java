package bio.terra.service.resourcemanagement;

import java.util.UUID;

public class BillingProfile {
    private UUID id;
    private String name;
    private String biller;
    private String billingAccountId;
    private boolean accessible;
    private String gcsRegion;

    public BillingProfile() {
    }

    public UUID getId() {
        return id;
    }

    public BillingProfile id(UUID id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public BillingProfile name(String name) {
        this.name = name;
        return this;
    }

    public String getBiller() {
        return biller;
    }

    public BillingProfile biller(String biller) {
        this.biller = biller;
        return this;
    }

    public String getBillingAccountId() {
        return billingAccountId;
    }

    public BillingProfile billingAccountId(String billingAccountId) {
        this.billingAccountId = billingAccountId;
        return this;
    }

    public boolean isAccessible() {
        return accessible;
    }

    public BillingProfile accessible(boolean accessible) {
        this.accessible = accessible;
        return this;
    }

    public String getGcsRegion() {
        return gcsRegion;
    }

    public BillingProfile gcsRegion(String gcsRegion) {
        this.gcsRegion = gcsRegion;
        return this;
    }
}
