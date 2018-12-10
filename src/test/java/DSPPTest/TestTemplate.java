package DSPPTest;

import org.junit.AfterClass;

public class TestTemplate {

    protected static double points = 0.0D;

    @AfterClass
    public static void summarize() {
        System.out.println("");
        System.out.println("Group achieved " + points + " points.");
        System.out.println("");
    }

}
