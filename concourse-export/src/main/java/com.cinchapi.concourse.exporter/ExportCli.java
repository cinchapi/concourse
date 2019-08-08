package com.cinchapi.concourse.exporter;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.cli.CommandLineInterface;
import jline.console.ConsoleReader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExportCli<E extends Exporter> extends CommandLineInterface<ExportOptions> {
    private final E exporter;
    private final Concourse concourse;

    public ExportCli(E exporter, Concourse concourse) {
        this.exporter = exporter;
        this.concourse = concourse;
    }

    @Override
    protected void doTask() {
        final Set<Long> recordIDs = options.records.size() > 0
                ? options.records
                : concourse.inventory();

        Path path = createFile();

        final Map<Long, Map<String, Object>> records = options.criteria != null
                ? concourse.get(options.criteria)
                : concourse.describe(recordIDs).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> concourse.get(e.getValue(), e.getKey())));
//                        : concourse.describe(recordIDs).entrySet().stream().map(entry -> {
//                            final long key = entry.getKey();
//                            final Set<String> value = entry.getValue();
//
//                            final Map<String, Object> $records = concourse.get(value, key);
//                            return new AbstractMap.SimpleImmutableEntry<>(key, $records);
//                        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final Iterable<Map<String, Object>> filteredRecords =
                records.entrySet().stream()
                        .filter(record -> recordIDs.contains(record.getKey()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        try {
            output(filteredRecords, Files.newOutputStream(path));
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

            if(path != null) {
                Files.createFile(path);
                return path;
            }
            else {
                System.out.println("Please enter a valid path.");
                return createFile();
            }
        } catch(IOException except) {
            throw new RuntimeException("Failed to interact with stdin.");
        }
    }

    private Path getPathOrNull(String path) {
        try {
            return Paths.get(path);
        } catch (InvalidPathException | NullPointerException ex) {
            return null;
        }
    }

    // TODO: Replace the functions below this line with the library function
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
