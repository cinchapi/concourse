import com.cinchapi.concourse.server.plugin.Plugin;
import INSERT_IMPORT_STATEMENT;

public class PluginLauncher {

    public static void main(String... args) throws InterruptedException {
        Object stopper = new Object();
        String mainSharedMemoryPath = "INSERT_SHARED_MEMORY_PATH";

        // #######################################################
        // ######## DO NOT EDIT ANYTHING BELOW THIS LINE #########
        // #######################################################

        Plugin plugin = new INSERT_CLASS_NAME(mainSharedMemoryPath, stopper);
        plugin.run();
        synchronized (stopper) {
            stopper.wait();
        }
        System.exit(0);
    }

}