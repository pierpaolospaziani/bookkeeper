package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.Journal.JournalIdFilter;
import org.junit.*;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class JournalListTest {
    private static final String validDirectory = "/tmp/journal";
    private static final String emptyDirectory = "/tmp/empty";
    private static final String invalidDirectory = "/...";
    private File journalDir;
    private JournalIdFilter filter;
    private final Class<? extends Exception> expectedException;
    private static final List<Long> expected = new ArrayList<>();
    private enum ObjType {
        NULL, VALID, INVALID, EMPTY
    }

    public JournalListTest(ObjType journalDirType, ObjType filterType, Class<? extends Exception> expectedException) {
        switch(journalDirType) {
            case NULL:
                this.journalDir = null;
                break;
            case VALID:
                this.journalDir = new File(validDirectory);
                break;
            case INVALID:
                this.journalDir = new File(invalidDirectory);
                break;
            case EMPTY:
                this.journalDir = new File(emptyDirectory);
                break;
        }
        switch(filterType) {
            case NULL:
                this.filter = null;
                break;
            case VALID:
                // Filtro di esempio per accettare solo ID pari
                this.filter = id -> id % 2 == 0;
                break;
            case INVALID:
                JournalIdFilter mockFilter = mock(JournalIdFilter.class);
                when(mockFilter.accept(anyLong())).thenReturn(false);
                this.filter = mockFilter;
                break;
        }
        this.expectedException = expectedException;
    }

    @BeforeClass
    public static void setup() throws IOException {
        File journalDir = new File(validDirectory);
        journalDir.mkdir();
        File emptyDir = new File(emptyDirectory);
        emptyDir.mkdir();

        // Creazione di alcuni file journal di esempio
        new File(journalDir + "/2.txn").createNewFile();
        new File(journalDir + "/3.txn").createNewFile();
        new File(journalDir + "/4.txn").createNewFile();
        new File(journalDir + "/otherfile.txt").createNewFile();
    }

    @Before
    public void putSomeEntries() {
        if (this.filter == null){
            expected.add(2L);
            expected.add(3L);
            expected.add(4L);
        } else {
            expected.add(2L);
            expected.add(4L);
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { ObjType.NULL,      ObjType.NULL,      NullPointerException.class },  // 0
                { ObjType.NULL,      ObjType.VALID,     NullPointerException.class },  // 1
                { ObjType.NULL,      ObjType.INVALID,   NullPointerException.class },  // 2
                { ObjType.VALID,     ObjType.NULL,      null                       },  // 3
                { ObjType.VALID,     ObjType.VALID,     null                       },  // 4
                { ObjType.VALID,     ObjType.INVALID,   null                       },  // 5
                { ObjType.INVALID,   ObjType.NULL,      null                       },  // 6
                { ObjType.INVALID,   ObjType.VALID,     null                       },  // 7
                { ObjType.INVALID,   ObjType.INVALID,   null                       },  // 8
                { ObjType.EMPTY,     ObjType.NULL,      null                       },  // 9
                { ObjType.EMPTY,     ObjType.VALID,     null                       },  // 10
                { ObjType.EMPTY,     ObjType.INVALID,   null                       }   // 11
        });
    }

    @Test
    public void testListJournalIds(){
        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                // Codice di test che non dovrebbe sollevare un'eccezione
                List<Long> result = Journal.listJournalIds(journalDir, filter);
                Assert.assertNotNull(result);
                if (!result.isEmpty()){
                    Assertions.assertEquals(expected, result);
                }
            });
        } else {
            Assertions.assertThrows(expectedException, () -> {
                // Codice di test che dovrebbe sollevare un'eccezione
                Journal.listJournalIds(journalDir, filter);
                Assertions.fail();
            });
        }
    }

    @After
    public void removeEntries() {
        expected.clear();
    }

    @AfterClass
    public static void cleanUp() {
        try {
            Files.delete(Paths.get(emptyDirectory));
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
}