package com.cinchapi.concourse.exporter;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.cli.CommandLineInterface;
import jline.console.ConsoleReader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExportCli extends CommandLineInterface<ExportOptions> {
    @Override
    protected void doTask() {
        final Set<Long> recordIDs = options.records.size() > 0
                ? options.records
                : concourse.inventory();

        Path path = createFile();

        final Iterable<Map<String, Object>> records = (options.criteria != null
                ? concourse.get(options.criteria).entrySet().stream()
                    .filter(r -> recordIDs.contains(r.getKey()))
                    .map(Map.Entry::getValue)
                : concourse.describe(recordIDs).entrySet().stream()
                    .map(e -> concourse.get(e.getValue(), e.getKey()))
                ).collect(Collectors.toList());

        try {
            output(records, Files.newOutputStream(path));
        } catch(IOException exception) {
            throw new RuntimeException("Failed to create a stream for the file.");
        }
    }

    @Override
    protected ExportOptions getOptions() {
        return new ExportOptions();
    }

    private Path createFile() {
        try {
            ConsoleReader reader = new ConsoleReader();
            System.out.println("What's the path of the file you want to create?");
            Path path = getPathOrNull(reader.readLine());

            if(path == null) {
                System.out.println("Please enter a valid path.");
            }
            return path == null ? createFile() : Files.createFile(path);
        } catch(IOException e) {
            throw new RuntimeException("Failed to interact with stdin.");
        }
    }

    private Path getPathOrNull(String path) {
        try {
            return Paths.get(path);
        } catch (InvalidPathException | NullPointerException e) {
            return null;
        }
    }

    // TODO: Replace the functions below this line with the library function
    // TODO: When https://github.com/cinchapi/accent4j/pull/10 is merged
    
    private static <T, Q> void output(Iterable<Map<T, Q>> items,
            OutputStream output) {
        PrintStream printer = new PrintStream(output);

        for(Map<T, Q> item : items) {
            for(Map.Entry<T, Q> entry : item.entrySet()) {
                outputItem(printer, entry.getKey(), entry.getValue());
            }
        }
    }

    private static <T, Q> void outputItem(PrintStream printer, T k, Q v) {
        if(v instanceof String) {
            printer.println(k + "," + AnyStrings
                    .ensureWithinQuotesIfNeeded((String) v, ','));
        }
        printer.println(k + "," + v);
    }

}
