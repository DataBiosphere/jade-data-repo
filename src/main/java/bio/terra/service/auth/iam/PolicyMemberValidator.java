package bio.terra.service.auth.iam;

import bio.terra.common.ValidationUtils;
import bio.terra.model.PolicyMemberRequest;
import javax.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class PolicyMemberValidator implements Validator {

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  private void ValidateEmail(String email, Errors errors) {
    if (email == null || email.isEmpty())
      errors.rejectValue("email", "EmailNotSupplied", "No email was supplied.");
    else if (!ValidationUtils.isValidEmail(email))
      errors.rejectValue("email", "InvalidEmailFormat", "The email supplied is not valid.");
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target != null && target instanceof PolicyMemberRequest) {
      PolicyMemberRequest policyMemberReq = (PolicyMemberRequest) target;
      ValidateEmail(policyMemberReq.getEmail(), errors);
    }
  }
}
