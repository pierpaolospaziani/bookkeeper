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
    private static final String invalidDirectory = "/...";
    private final Journal journal;
    private final long journalId;
    private final long journalPos;
    private JournalScanner scanner;
    private final Class<? extends Exception> expectedException;
    private enum ObjType {
        NULL, VALID, INVALID
    }

    static final List<File> tempDirs = new ArrayList<>();

    public ScanJournalTest(long journalId, long journalPos, ObjType scannerType, Class<? extends Exception> expectedException) throws Exception {
        this.journal = generateJournal();
        this.journalId = journalId;
        this.journalPos = journalPos;
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
                { -1,    -1,    ObjType.NULL,       null },  // 0
                { -1,    -1,    ObjType.VALID,      null },  // 1
                { -1,    -1,    ObjType.INVALID,    null },  // 2
                { -1,     0,    ObjType.NULL,       null },  // 3
                { -1,     0,    ObjType.VALID,      null },  // 4
                { -1,     0,    ObjType.INVALID,    null },  // 5
                { -1,     1,    ObjType.NULL,       null },  // 6
                { -1,     1,    ObjType.VALID,      null },  // 7
                { -1,     1,    ObjType.INVALID,    null },  // 8
                {  0,    -1,    ObjType.NULL,       null },  // 9
                {  0,    -1,    ObjType.VALID,      null },  // 10
                {  0,    -1,    ObjType.INVALID,    null },  // 11
                {  0,     0,    ObjType.NULL,       null },  // 12
                {  0,     0,    ObjType.VALID,      null },  // 13
                {  0,     0,    ObjType.INVALID,    null },  // 14
                {  0,     1,    ObjType.NULL,       null },  // 15
                {  0,     1,    ObjType.VALID,      null },  // 16
                {  0,     1,    ObjType.INVALID,    null },  // 17
                {  1,    -1,    ObjType.NULL,       null },  // 18
                {  1,    -1,    ObjType.VALID,      null },  // 19
                {  1,    -1,    ObjType.INVALID,    null },  // 20
                {  1,     0,    ObjType.NULL,       null },  // 21
                {  1,     0,    ObjType.VALID,      null },  // 22
                {  1,     0,    ObjType.INVALID,    null },  // 23
                {  1,     1,    ObjType.NULL,       null },  // 24
                {  1,     1,    ObjType.VALID,      null },  // 25
                {  1,     1,    ObjType.INVALID,    null }   // 26
        });
    }

    @Test
    public void testListJournalIds() {

//        long generatedJournalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
//        long generatedOutput = this.journal.scanJournal(generatedJournalId, this.journalPos, this.scanner);

        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                long requestedOutput = this.journal.scanJournal(this.journalId, this.journalPos, this.scanner);
                long validOutput = this.journal.scanJournal(Journal.listJournalIds(new File(validDirectory), null).get(0), this.journalPos, this.scanner);
                Assertions.assertTrue(requestedOutput >= validOutput);
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
