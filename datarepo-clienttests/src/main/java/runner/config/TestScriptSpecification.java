package runner.config;

import runner.TestScript;

import java.util.concurrent.TimeUnit;

public class TestScriptSpecification implements SpecificationInterface {
    public String name;
    public int totalNumberToRun;
    public int numberToRunInParallel;
    public long expectedTimeForEach;
    public String expectedTimeForEachUnit;

    public Class<? extends TestScript> scriptClass;
    public TimeUnit expectedTimeForEachUnitObj;

    public static final String scriptsPackage = "testscripts";

    TestScriptSpecification() { }

    /**
     * Validate the server specification read in from the JSON file.
     * The timeout string is parsed into a Duration; the name is converted into a Java class reference.
     */
    public void validate() {
        if (totalNumberToRun <= 0) {
            throw new IllegalArgumentException("Total number to run must be >=0.");
        }
        if (numberToRunInParallel <= 0) {
            throw new IllegalArgumentException("Number to run in parallel must be >=0.");
        }
        if (expectedTimeForEach <= 0) {
            throw new IllegalArgumentException("Expected time for each must be >=0.");
        }

        expectedTimeForEachUnitObj = TimeUnit.valueOf(expectedTimeForEachUnit);

        try {
            Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
            scriptClass = (Class<? extends TestScript>)scriptClassGeneric;
        } catch (ClassNotFoundException|ClassCastException classEx) {
            throw new IllegalArgumentException("Test script class not found: " + name, classEx);
        }
    }

    public void display() {
        System.out.println("Test Script: " + name);
        System.out.println("  totalNumberToRun: " + totalNumberToRun);
        System.out.println("  numberToRunInParallel: " + numberToRunInParallel);
        System.out.println("  expectedTimeForEach: " + expectedTimeForEach);
        System.out.println("  expectedTimeForEachUnit: " + expectedTimeForEachUnitObj);
    }
}
