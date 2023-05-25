package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
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
    private final ByteBuf entry;
    private final Class<? extends Exception> expectedException;

    public ReadCachePutTest(long ledgerId, long entryId, ByteBuf entry, Class<? extends Exception> expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.entry = entry;
        this.expectedException = expectedException;
    }

    @Before
    public  void configure() {
        cache = new ReadCache(allocator, cacheCapability);
    }

    @Parameters
    public static Collection<Object[]> data() {
        ByteBuf validEntry = allocator.buffer(entrySize);
        validEntry.writerIndex(validEntry.capacity());
        ByteBuf invalidEntry = allocator.buffer();

        return Arrays.asList(new Object[][] {
                {   -1,    -1,    null,          NullPointerException.class         },  // 0
                {   -1,    -1,    validEntry,    IllegalArgumentException.class     },  // 1
                {   -1,    -1,    invalidEntry,  IllegalArgumentException.class     },  // 2
                {   -1,     0,    null,          NullPointerException.class         },  // 3
                {   -1,     0,    validEntry,    IllegalArgumentException.class     },  // 4
                {   -1,     0,    invalidEntry,  IllegalArgumentException.class     },  // 5
                {   -1,     1,    null,          NullPointerException.class         },  // 6
                {   -1,     1,    validEntry,    IllegalArgumentException.class     },  // 7
                {   -1,     1,    invalidEntry,  IllegalArgumentException.class     },  // 8
                {   0,    -1,     null,          NullPointerException.class         },  // 9
                {   0,    -1,     validEntry,    null                               },  // 10
                {   0,    -1,     invalidEntry,  null                               },  // 11
                {   0,     0,     null,          NullPointerException.class         },  // 12
                {   0,     0,     validEntry,    null                               },  // 13
                {   0,     0,     invalidEntry,  null                               },  // 14
                {   0,     1,     null,          NullPointerException.class         },  // 15
                {   0,     1,     validEntry,    null                               },  // 16
                {   0,     1,     invalidEntry,  null                               },  // 17
                {   1,    -1,     null,          NullPointerException.class         },  // 18
                {   1,    -1,     validEntry,    null                               },  // 19
                {   1,    -1,     invalidEntry,  null                               },  // 20
                {   1,     0,     null,          NullPointerException.class         },  // 21
                {   1,     0,     validEntry,    null                               },  // 22
                {   1,     0,     invalidEntry,  null                               },  // 23
                {   1,     1,     null,          NullPointerException.class         },  // 24
                {   1,     1,     validEntry,    null                               },  // 25
                {   1,     1,     invalidEntry,  null                               }   // 26
        });
    }

    @Test
    public void putTest() {

        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                // Codice di test che non dovrebbe sollevare un'eccezione

                assertEquals(0, cache.count());

                cache.put(ledgerId, entryId, entry);

                assertEquals(1,cache.count());
            });
        } else {
            Assertions.assertThrows(expectedException, () -> {
                // Codice di test che dovrebbe sollevare un'eccezione
                cache.put(ledgerId, entryId, entry);
            });
        }
    }

    @After
    public void cleanUp() {
        cache.close();
    }
}