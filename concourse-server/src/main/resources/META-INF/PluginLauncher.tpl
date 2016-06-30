import com.cinchapi.concourse.server.plugin.Plugin;
import INSERT_IMPORT_STATEMENT;

public class PluginLauncher {

    public static void main(String... args) throws InterruptedException {
        String serverLoop = "INSERT_SERVER_LOOP";
        String pluginLoop = "INSERT_PLUGIN_LOOP";

        // #######################################################
        // ######## DO NOT EDIT ANYTHING BELOW THIS LINE #########
        // #######################################################

        Plugin plugin = new INSERT_CLASS_NAME(serverLoop, pluginLoop);
        plugin.run();
        synchronized (stopper) {
            stopper.wait();
        }
        System.exit(0);
    }

}