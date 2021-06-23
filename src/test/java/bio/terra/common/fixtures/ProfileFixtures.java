package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public final class ProfileFixtures {
    private ProfileFixtures() {}
    private static SecureRandom randomGenerator = new SecureRandom();

    public static String randomHex(int n) {
        Random r = new Random();
        StringBuffer sb = new StringBuffer();
        while (sb.length() < n) {
            sb.append(Integer.toHexString(r.nextInt(16)));
        }
        return sb.toString();
    }

    // Google billing account ids are three sets of six capitalized hex characters separated by dashes
    public static String randomBillingAccountId() {
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            groups.add(randomHex(6));
        }
        return String.join("-", groups).toUpperCase();
    }

    public static BillingProfileModel billingProfileForAccount(final String accountId) {
        return new BillingProfileModel()
            .id(UUID.randomUUID())
            .billingAccountId(accountId)
            .profileName(randomizeName("test-profile"))
            .biller("direct")
            .description("test profile description");
    }

    public static BillingProfileModel randomBillingProfile() {
        return billingProfileForAccount(randomBillingAccountId());
    }

    public static BillingProfileRequestModel billingProfileRequest(final BillingProfileModel profile) {
        return new BillingProfileRequestModel()
            .id(profile.getId())
            .biller(profile.getBiller())
            .profileName(profile.getProfileName())
            .billingAccountId(profile.getBillingAccountId())
            .description(profile.getDescription());
    }

    public static BillingProfileRequestModel randomBillingProfileRequest() {
        return billingProfileRequest(randomBillingProfile());
    }

    public static String randomizeName(String baseName) {
        long suffix = randomGenerator.nextLong();
        return baseName + Long.toUnsignedString(suffix);
    }

}
