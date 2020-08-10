package runner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
    value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class DisruptionResult {
    public String description;

    public String threadName;

    public boolean completed;
    public Exception exceptionThrown;

    public DisruptionResult(String description, String threadName) {
        this.description = description;
        this.threadName = threadName;

        this.exceptionThrown = null;
        this.completed = false;
    }
}
