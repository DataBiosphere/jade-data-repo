package bio.terra.service.profile;

import bio.terra.model.BillingProfileUpdateModel;
import javax.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class ProfileUpdateRequestValidator implements Validator {
  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target != null && target instanceof BillingProfileUpdateModel) {
      BillingProfileUpdateModel billingProfileUpdateModel = (BillingProfileUpdateModel) target;
      String billingAccountId = billingProfileUpdateModel.getBillingAccountId();
      if (billingAccountId == null || !ProfileRequestValidator.isValidAccountId(billingAccountId)) {
        errors.rejectValue(
            "billingAccountId",
            "The id must be 3 sets of 6 capitalized alphanumeric characters separated by dashes");
      }
      if (billingProfileUpdateModel.getId() == null) {
        errors.rejectValue("id", "The billing profile id must be specified");
      }
    }
  }
}
