import java.io.Serializable;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.io.process.Forkable;
import com.cinchapi.concourse.util.Serializables;

public class ForkRunner {

    @SuppressWarnings("unchecked")
    public static void main(String... args) throws Exception {
        String input = "INSERT_INPUT_PATH";
        String output = "INSERT_OUTPUT_PATH";
        String clazz = "INSERT_CLASS_NAME";
        Forkable<? extends Serializable> forkable = Serializables.read(
                FileSystem.readBytes(input), clazz);
        Serializable result = forkable.call();
        FileSystem.writeBytes(Serializables.getBytes(result), output);
    }

}