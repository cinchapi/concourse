package org.cinchapi.concourse.importer;

import org.cinchapi.concourse.util.Resources;
import org.junit.Test;

public class InvalidCsvFileFormatImportTest extends CsvImportTest {

    @Override
    protected String getImportPath() {
        return "invalid.csv";
    }

    @Override
    @Test(expected = IllegalArgumentException.class)
    public void testImport() {
        String file = Resources.get("/" + getImportPath()).getFile();
        importer.importFile(file);
    }

}
