package bio.terra.stairway;

final class TestUtil {
    private TestUtil() {
    }

    static final Integer intValue = Integer.valueOf(22);
    static final String strValue = "testing 1 2 3";
    static final Double dubValue = new Double(Math.PI);
    static final String errString = "Something bad happened";
    static final String flightId = "aaa111";
    static final String ikey = "ikey";
    static final String skey = "skey";
    static final String fkey = "fkey";
    static final String wikey = "wikey";
    static final String wskey = "wskey";
    static final String wfkey = "wfkey";

    // debug output control (until we have logging configured)
    static final boolean debugOutput = false;

    static void debugWrite(String msg) {
        if (debugOutput) {
            System.out.println(msg);
        }
    }
}
