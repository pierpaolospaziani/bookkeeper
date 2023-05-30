package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ReadCachePutTest {
    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private static final int entrySize = 1024;
    private static final int cacheCapability = 2 * entrySize;

    private static ReadCache cache;

    private final long ledgerId;
    private final long entryId;
    private ByteBuf entry;
    private final Class<? extends Exception> expectedException;
    private enum ObjType {
        NULL, VALID, INVALID
    }

    public ReadCachePutTest(long ledgerId, long entryId, ObjType entryType, Class<? extends Exception> expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.expectedException = expectedException;
        switch(entryType) {
            case NULL:
                this.entry = null;
                break;
            case VALID:
                ByteBuf validEntry = allocator.buffer(entrySize);
                validEntry.writerIndex(validEntry.capacity());
                this.entry = validEntry;
                break;
            case INVALID:
                ByteBuf invalidEntry = allocator.buffer(4*entrySize);
                invalidEntry.writerIndex(invalidEntry.capacity());
                this.entry = invalidEntry;
                break;
        }
    }

    @Before
    public  void configure() {
        cache = new ReadCache(allocator, cacheCapability);
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { -1,    -1,    ObjType.NULL,       NullPointerException.class     },  // 0
                { -1,    -1,    ObjType.VALID,      IllegalArgumentException.class },  // 1
                { -1,    -1,    ObjType.INVALID,    null                           },  // 2
                { -1,     0,    ObjType.NULL,       NullPointerException.class     },  // 3
                { -1,     0,    ObjType.VALID,      IllegalArgumentException.class },  // 4
                { -1,     0,    ObjType.INVALID,    null                           },  // 5
                { -1,     1,    ObjType.NULL,       NullPointerException.class     },  // 6
                { -1,     1,    ObjType.VALID,      IllegalArgumentException.class },  // 7
                { -1,     1,    ObjType.INVALID,    null                           },  // 8
                {  0,    -1,    ObjType.NULL,       NullPointerException.class     },  // 9
                {  0,    -1,    ObjType.VALID,      null                           },  // 10
                {  0,    -1,    ObjType.INVALID,    null                           },  // 11
                {  0,     0,    ObjType.NULL,       NullPointerException.class     },  // 12
                {  0,     0,    ObjType.VALID,      null                           },  // 13
                {  0,     0,    ObjType.INVALID,    null                           },  // 14
                {  0,     1,    ObjType.NULL,       NullPointerException.class     },  // 15
                {  0,     1,    ObjType.VALID,      null                           },  // 16
                {  0,     1,    ObjType.INVALID,    null                           },  // 17
                {  1,    -1,    ObjType.NULL,       NullPointerException.class     },  // 18
                {  1,    -1,    ObjType.VALID,      null                           },  // 19
                {  1,    -1,    ObjType.INVALID,    null                           },  // 20
                {  1,     0,    ObjType.NULL,       NullPointerException.class     },  // 21
                {  1,     0,    ObjType.VALID,      null                           },  // 22
                {  1,     0,    ObjType.INVALID,    null                           },  // 23
                {  1,     1,    ObjType.NULL,       NullPointerException.class     },  // 24
                {  1,     1,    ObjType.VALID,      null                           },  // 25
                {  1,     1,    ObjType.INVALID,    null                           }   // 26
        });
    }

    @Test
    public void putTest() {
        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                // Codice di test che non dovrebbe sollevare un'eccezione

                assertEquals(0, cache.count());

                cache.put(ledgerId, entryId, entry);

                if (entry.capacity() == entrySize){
                    assertEquals(1,cache.count());
                    cache.put(ledgerId, entryId, entry);
                    assertEquals(2,cache.count());
                }
            });
        } else {
            Assertions.assertThrows(expectedException, () -> {
                // Codice di test che dovrebbe sollevare un'eccezione
                cache.put(ledgerId, entryId, entry);
                Assertions.fail();
            });
        }
    }

    @After
    public void cleanUp() {
        cache.close();
    }
}