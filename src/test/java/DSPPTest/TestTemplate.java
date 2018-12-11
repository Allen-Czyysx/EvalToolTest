package DSPPTest;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTemplate {

    protected static double points = 0.0D;

    @BeforeClass
    public static void init() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    @AfterClass
    public static void summarize() {
        System.out.println("");
        System.out.println("Group achieved " + points + " points.");
        System.out.println("");
    }

}
