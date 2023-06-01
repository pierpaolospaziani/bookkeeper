package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ScanJournalTest {
    private static final String validDirectory = "/tmp/journal";
    private final Journal journal;
    private long journalId;
    private final long journalPos;
    private JournalScanner scanner;
    private final Class<? extends Exception> expectedException;
    private enum idType {
        NEGATIVEONE, ZERO, ONE, VALID
    }
    private enum ObjType {
        NULL, VALID, INVALID
    }

    static final List<File> tempDirs = new ArrayList<>();

    public ScanJournalTest(idType journalIdType, long journalPos, ObjType scannerType, Class<? extends Exception> expectedException) throws Exception {
        this.journal = generateJournal();
        this.journalPos = journalPos;
        switch(journalIdType) {
            case NEGATIVEONE:
                this.journalId = -1;
                break;
            case ZERO:
                this.journalId = 0;
                break;
            case ONE:
                this.journalId = 1;
                break;
            case VALID:
                this.journalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
                break;
        }
        switch(scannerType) {
            case NULL:
                this.scanner = null;
                break;
            case VALID:
                this.scanner = new ValidJournalScanner();
                break;
            case INVALID:
                this.scanner = new InvalidJournalScanner();
                break;
        }
        this.expectedException = expectedException;
    }

    public Journal generateJournal() throws Exception {
        File journalDir = JournalUtils.createTempDir(tempDirs, "journal");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = JournalUtils.createTempDir(tempDirs, "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));
        JournalUtils.writeV4Journal(BookieImpl.getCurrentDirectory(journalDir), "testPasswd".getBytes());
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);
        return new TestBookieImpl(conf).journals.get(0);
    }

    @BeforeClass
    public static void addSomeJournals() throws IOException {
        File journalDir = new File(validDirectory);
        journalDir.mkdir();

        // Creazione di un file journal di esempio
        new File(journalDir + "/2.txn").createNewFile();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {  idType.NEGATIVEONE,   -1,    ObjType.NULL,       null                       },  // 0
                {  idType.NEGATIVEONE,   -1,    ObjType.VALID,      null                       },  // 1
                {  idType.NEGATIVEONE,   -1,    ObjType.INVALID,    null                       },  // 2
                {  idType.NEGATIVEONE,    0,    ObjType.NULL,       null                       },  // 3
                {  idType.NEGATIVEONE,    0,    ObjType.VALID,      null                       },  // 4
                {  idType.NEGATIVEONE,    0,    ObjType.INVALID,    null                       },  // 5
                {  idType.NEGATIVEONE,    1,    ObjType.NULL,       null                       },  // 6
                {  idType.NEGATIVEONE,    1,    ObjType.VALID,      null                       },  // 7
                {  idType.NEGATIVEONE,    1,    ObjType.INVALID,    null                       },  // 8
                {  idType.ZERO,          -1,    ObjType.NULL,       null                       },  // 9
                {  idType.ZERO,          -1,    ObjType.VALID,      null                       },  // 10
                {  idType.ZERO,          -1,    ObjType.INVALID,    null                       },  // 11
                {  idType.ZERO,           0,    ObjType.NULL,       null                       },  // 12
                {  idType.ZERO,           0,    ObjType.VALID,      null                       },  // 13
                {  idType.ZERO,           0,    ObjType.INVALID,    null                       },  // 14
                {  idType.ZERO,           1,    ObjType.NULL,       null                       },  // 15
                {  idType.ZERO,           1,    ObjType.VALID,      null                       },  // 16
                {  idType.ZERO,           1,    ObjType.INVALID,    null                       },  // 17
                {  idType.ONE,           -1,    ObjType.NULL,       null                       },  // 18
                {  idType.ONE,           -1,    ObjType.VALID,      null                       },  // 19
                {  idType.ONE,           -1,    ObjType.INVALID,    null                       },  // 20
                {  idType.ONE,            0,    ObjType.NULL,       null                       },  // 21
                {  idType.ONE,            0,    ObjType.VALID,      null                       },  // 22
                {  idType.ONE,            0,    ObjType.INVALID,    null                       },  // 23
                {  idType.ONE,            1,    ObjType.NULL,       null                       },  // 24
                {  idType.ONE,            1,    ObjType.VALID,      null                       },  // 25
                {  idType.ONE,            1,    ObjType.INVALID,    null                       },  // 26
                {  idType.VALID,         -1,    ObjType.NULL,       NullPointerException.class },  // 27
                {  idType.VALID,         -1,    ObjType.VALID,      null                       },  // 28
                {  idType.VALID,         -1,    ObjType.INVALID,    RuntimeException.class     },  // 29
                {  idType.VALID,          0,    ObjType.NULL,       NullPointerException.class },  // 30
                {  idType.VALID,          0,    ObjType.VALID,      null                       },  // 31
                {  idType.VALID,          0,    ObjType.INVALID,    RuntimeException.class     },  // 32
                {  idType.VALID,          1,    ObjType.NULL,       null                       },  // 33
                {  idType.VALID,          1,    ObjType.VALID,      null                       },  // 34
                {  idType.VALID,          1,    ObjType.INVALID,    null                       },  // 35
        });
    }

    @Test
    public void testScanJournal() {
        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                long requestedOutput = this.journal.scanJournal(this.journalId, this.journalPos, this.scanner);
                long dummyOutput = this.journal.scanJournal(Journal.listJournalIds(new File(validDirectory), null).get(0), this.journalPos, this.scanner);
                Assertions.assertTrue(requestedOutput >= dummyOutput);
            });
        } else {
            Assertions.assertThrows(expectedException, () -> {
                this.journal.scanJournal(this.journalId, this.journalPos, this.scanner);
                Assertions.fail();
            });
        }
    }

    @AfterClass
    public static void cleanUp() {
        try {
            Files.walkFileTree(Paths.get(validDirectory), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (Exception ignored){}
    }

    private static class ValidJournalScanner implements JournalScanner {
        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {}
    }

    private static class InvalidJournalScanner implements JournalScanner {
        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            throw new RuntimeException();
        }
    }
}
