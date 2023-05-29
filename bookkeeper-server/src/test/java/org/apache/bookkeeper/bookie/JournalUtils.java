package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class JournalUtils {
    private JournalUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static File createTempDir(List<File> tempDirs, String suffix) throws IOException {
        File dir = IOUtils.createTempDir("bookie", suffix);
        tempDirs.add(dir);
        return dir;
    }

    static void writeV4Journal(File journalDir, byte[] masterKey) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 0; i <= 100; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = ClientUtil.generatePacket(1, i, lastConfirmed, i * data.length, data);
            }
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();
            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            ReferenceCountUtil.release(packet);
        }
        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        bc.write(lenBuf);
        bc.write(packet);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V4);
    }

    private static void moveToPosition(JournalChannel jc, long pos) throws IOException {
        jc.fc.position(pos);
        jc.bc.position = pos;
        jc.bc.writeBufferStartPosition.set(pos);
    }

    private static ByteBuf generateMetaEntry(long ledgerId, byte[] masterKey) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_LEDGER_KEY);
        bb.writeInt(masterKey.length);
        bb.writeBytes(masterKey);
        return bb;
    }

    private static ByteBuf generateFenceEntry(long ledgerId) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_FENCE_KEY);
        return bb;
    }

    private static void updateJournalVersion(JournalChannel jc, int journalVersion) throws IOException {
        long prevPos = jc.fc.position();
        try {
            ByteBuffer versionBuffer = ByteBuffer.allocate(4);
            versionBuffer.putInt(journalVersion);
            versionBuffer.flip();
            jc.fc.position(4);
            IOUtils.writeFully(jc.fc, versionBuffer);
            jc.fc.force(true);
        } finally {
            jc.fc.position(prevPos);
        }
    }
}
