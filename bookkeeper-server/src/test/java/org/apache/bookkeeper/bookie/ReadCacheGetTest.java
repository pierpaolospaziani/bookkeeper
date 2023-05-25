package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class ReadCacheGetTest {
    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private static final int entrySize = 1024;
    private static final int cacheCapability = 2 * entrySize;

    private static ReadCache cache;

    private final long ledgerId;
    private final long entryId;
    private final Class<? extends Exception> expectedException;

    public ReadCacheGetTest(long ledgerId, long entryId, Class<? extends Exception> expectedException) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.expectedException = expectedException;
    }

    @Before
    public  void configure() {
        cache = new ReadCache(allocator, cacheCapability);

        // è stata inserita in cache un ByteBuf dummy per test
        ByteBuf byteBuf = Unpooled.buffer(16);
        byteBuf.writeBytes("Hello, World!".getBytes());
        cache.put(1, 1, byteBuf);
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { -1, -1,  IllegalArgumentException.class  },  // 0
                { -1,  0,  IllegalArgumentException.class  },  // 1
                { -1,  1,  IllegalArgumentException.class  },  // 2
                {  0, -1,  null                            },  // 3
                {  0,  0,  null                            },  // 4
                {  0,  1,  null                            },  // 5
                {  1, -1,  null                            },  // 6
                {  1,  0,  null                            },  // 7
                {  1,  1,  null                            }   // 8
        });
    }

    @Test
    public void getTest() {

        if (expectedException == null) {
            Assertions.assertDoesNotThrow(() -> {
                // Codice di test che non dovrebbe sollevare un'eccezione
                ByteBuf byteBuf = cache.get(ledgerId, entryId);
                if (ledgerId == 1 && entryId == 1){
                    assertEquals("Hello, World!", byteBuf.toString(Charset.defaultCharset()));
                } else {
                    assertNull(byteBuf);
                }
            });
        } else {
            Assertions.assertThrows(expectedException, () -> {
                // Codice di test che dovrebbe sollevare un'eccezione
                cache.get(ledgerId, entryId);
            });
        }
    }

    @After
    public void cleanUp() {
        cache.close();
    }
}