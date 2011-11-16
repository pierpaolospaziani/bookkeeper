package org.apache.bookkeeper.client;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Admin client for BookKeeper clusters
 */
public class BookKeeperAdmin {
    private static Logger LOG = Logger.getLogger(BookKeeperAdmin.class);

    static final String COLON = ":";

    // ZK client instance
    private ZooKeeper zk;
    // ZK ledgers related String constants
    static final String LEDGERS_PATH = "/ledgers";
    static final String LEDGER_NODE_PREFIX = "L";
    static final String AVAILABLE_NODE = "available";
    static final String BOOKIES_PATH = LEDGERS_PATH + "/" + AVAILABLE_NODE;

    // BookKeeper client instance
    private BookKeeper bkc;

    /*
     * Random number generator used to choose an available bookie server to
     * replicate data from a dead bookie.
     */
    private Random rand = new Random();

    /*
     * For now, assume that all ledgers were created with the same DigestType
     * and password. In the future, this admin tool will need to know for each
     * ledger, what was the DigestType and password used to create it before it
     * can open it. These values will come from System properties, though hard
     * coded defaults are defined here.
     */
    private DigestType DIGEST_TYPE = DigestType.valueOf(System.getProperty("digestType", DigestType.CRC32.toString()));
    private byte[] PASSWD = System.getProperty("passwd", "").getBytes();

    /**
     * Constructor that takes in a ZooKeeper servers connect string so we know
     * how to connect to ZooKeeper to retrieve information about the BookKeeper
     * cluster. We need this before we can do any type of admin operations on
     * the BookKeeper cluster.
     *
     * @param zkServers
     *            Comma separated list of hostname:port pairs for the ZooKeeper
     *            servers cluster.
     * @throws IOException
     *             throws this exception if there is an error instantiating the
     *             ZooKeeper client.
     * @throws InterruptedException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     * @throws KeeperException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(String zkServers) throws IOException, InterruptedException, KeeperException {
        // Create the ZooKeeper client instance
        zk = new ZooKeeper(zkServers, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Process: " + event.getType() + " " + event.getPath());
                }
            }
        });
        // Create the BookKeeper client instance
        bkc = new BookKeeper(zk);
    }

    /**
     * Gracefully release resources that this client uses.
     *
     * @throws InterruptedException
     *             if there is an error shutting down the clients that this
     *             class uses.
     */
    public void close() throws InterruptedException, BKException {
        bkc.close();
        zk.close();
    }

    /**
     * This is a multi callback object for bookie recovery that waits for all of
     * the multiple async operations to complete. If any fail, then we invoke
     * the final callback with a BK LedgerRecoveryException.
     */
    class MultiCallback implements AsyncCallback.VoidCallback {
        // Number of expected callbacks
        final int expected;
        // Final callback and the corresponding context to invoke
        final AsyncCallback.VoidCallback cb;
        final Object context;
        // This keeps track of how many operations have completed
        final AtomicInteger done = new AtomicInteger();
        // List of the exceptions from operations that completed unsuccessfully
        final LinkedBlockingQueue<Integer> exceptions = new LinkedBlockingQueue<Integer>();

        MultiCallback(int expected, AsyncCallback.VoidCallback cb, Object context) {
            this.expected = expected;
            this.cb = cb;
            this.context = context;
            if (expected == 0) {
                cb.processResult(Code.OK.intValue(), null, context);
            }
        }

        private void tick() {
            if (done.incrementAndGet() == expected) {
                if (exceptions.isEmpty()) {
                    cb.processResult(Code.OK.intValue(), null, context);
                } else {
                    cb.processResult(BKException.Code.LedgerRecoveryException, null, context);
                }
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != Code.OK.intValue()) {
                LOG.error("BK error recovering ledger data", BKException.create(rc));
                exceptions.add(rc);
            }
            tick();
        }

    }

    /**
     * Method to get the input ledger's digest type. For now, this is just a
     * placeholder function since there is no way we can get this information
     * easily. In the future, BookKeeper should store this ledger metadata
     * somewhere such that an admin tool can access it.
     *
     * @param ledgerId
     *            LedgerId we are retrieving the digestType for.
     * @return DigestType for the input ledger
     */
    private DigestType getLedgerDigestType(long ledgerId) {
        return DIGEST_TYPE;
    }

    /**
     * Method to get the input ledger's password. For now, this is just a
     * placeholder function since there is no way we can get this information
     * easily. In the future, BookKeeper should store this ledger metadata
     * somewhere such that an admin tool can access it.
     *
     * @param ledgerId
     *            LedgerId we are retrieving the password for.
     * @return Password for the input ledger
     */
    private byte[] getLedgerPasswd(long ledgerId) {
        return PASSWD;
    }

    // Object used for calling async methods and waiting for them to complete.
    class SyncObject {
        boolean value;
        int rc;

        public SyncObject() {
            value = false;
            rc = BKException.Code.OK;
        }
    }

    /**
     * Synchronous method to rebuild and recover the ledger fragments data that
     * was stored on the source bookie. That bookie could have failed completely
     * and now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     */
    public void recoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest)
            throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(bookieSrc, bookieDest, new RecoverCallback() {
            @Override
            public void recoverComplete(int rc, Object ctx) {
                LOG.info("Recover bookie operation completed with rc: " + rc);
                SyncObject syncObj = (SyncObject) ctx;
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }
        }, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
    }

    /**
     * Async method to rebuild and recover the ledger fragments data that was
     * stored on the source bookie. That bookie could have failed completely and
     * now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                       final RecoverCallback cb, final Object context) {
        // Sync ZK to make sure we're reading the latest bookie/ledger data.
        zk.sync(LEDGERS_PATH, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing: ", KeeperException.create(KeeperException.Code.get(rc), path));
                    cb.recoverComplete(BKException.Code.ZKException, context);
                    return;
                }
                getAvailableBookies(bookieSrc, bookieDest, cb, context);
            };
        }, null);
    }

    /**
     * This method asynchronously gets the set of available Bookies that the
     * dead input bookie's data will be copied over into. If the user passed in
     * a specific destination bookie, then just use that one. Otherwise, we'll
     * randomly pick one of the other available bookies to use for each ledger
     * fragment we are replicating.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    private void getAvailableBookies(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                     final RecoverCallback cb, final Object context) {
        final List<InetSocketAddress> availableBookies = new LinkedList<InetSocketAddress>();
        if (bookieDest != null) {
            availableBookies.add(bookieDest);
            // Now poll ZK to get the active ledgers
            getActiveLedgers(bookieSrc, bookieDest, cb, context, availableBookies);
        } else {
            zk.getChildren(BOOKIES_PATH, null, new AsyncCallback.ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    if (rc != Code.OK.intValue()) {
                        LOG.error("ZK error getting bookie nodes: ", KeeperException.create(KeeperException.Code
                                  .get(rc), path));
                        cb.recoverComplete(BKException.Code.ZKException, context);
                        return;
                    }
                    for (String bookieNode : children) {
                        String parts[] = bookieNode.split(COLON);
                        if (parts.length < 2) {
                            LOG.error("Bookie Node retrieved from ZK has invalid name format: " + bookieNode);
                            cb.recoverComplete(BKException.Code.ZKException, context);
                            return;
                        }
                        availableBookies.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                    }
                    // Now poll ZK to get the active ledgers
                    getActiveLedgers(bookieSrc, bookieDest, cb, context, availableBookies);
                }
            }, null);
        }
    }

    /**
     * This method asynchronously polls ZK to get the current set of active
     * ledgers. From this, we can open each ledger and look at the metadata to
     * determine if any of the ledger fragments for it were stored at the dead
     * input bookie.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     * @param availableBookies
     *            List of Bookie Servers that are available to use for
     *            replicating data on the failed bookie. This could contain a
     *            single bookie server if the user explicitly chose a bookie
     *            server to replicate data to.
     */
    private void getActiveLedgers(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                  final RecoverCallback cb, final Object context, final List<InetSocketAddress> availableBookies) {
        zk.getChildren(LEDGERS_PATH, null, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error getting ledger nodes: ", KeeperException.create(KeeperException.Code.get(rc),
                              path));
                    cb.recoverComplete(BKException.Code.ZKException, context);
                    return;
                }
                // Wrapper class around the RecoverCallback so it can be used
                // as the final VoidCallback to invoke within the MultiCallback.
                class RecoverCallbackWrapper implements AsyncCallback.VoidCallback {
                    final RecoverCallback cb;

                    RecoverCallbackWrapper(RecoverCallback cb) {
                        this.cb = cb;
                    }

                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        cb.recoverComplete(rc, ctx);
                    }
                }
                // Recover each of the ledgers asynchronously
                MultiCallback ledgerMcb = new MultiCallback(children.size(), new RecoverCallbackWrapper(cb), context);
                for (final String ledgerNode : children) {
                    recoverLedger(bookieSrc, ledgerNode, ledgerMcb, availableBookies);
                }
            }
        }, null);
    }

    /**
     * Get a new random bookie, but ensure that it isn't one that is already
     * in the ensemble for the ledger.
     */
    private InetSocketAddress getNewBookie(final List<InetSocketAddress> bookiesAlreadyInEnsemble, 
                                           final List<InetSocketAddress> availableBookies) 
            throws BKException.BKNotEnoughBookiesException {
        ArrayList<InetSocketAddress> candidates = new ArrayList<InetSocketAddress>();
        candidates.addAll(availableBookies);
        candidates.removeAll(bookiesAlreadyInEnsemble);
        if (candidates.size() == 0) {
            throw new BKException.BKNotEnoughBookiesException();
        }
        return candidates.get(rand.nextInt(candidates.size()));
    }

    /**
     * This method asynchronously recovers a given ledger if any of the ledger
     * entries were stored on the failed bookie.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param ledgerNode
     *            Ledger Node name as retrieved from ZooKeeper we want to
     *            recover.
     * @param ledgerMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger.
     * @param availableBookies
     *            List of Bookie Servers that are available to use for
     *            replicating data on the failed bookie. This could contain a
     *            single bookie server if the user explicitly chose a bookie
     *            server to replicate data to.
     */
    private void recoverLedger(final InetSocketAddress bookieSrc, final String ledgerNode,
                               final MultiCallback ledgerMcb, final List<InetSocketAddress> availableBookies) {
        /*
         * The available node is also stored in this path so ignore that. That
         * node is the path for the set of available Bookie Servers.
         */
        if (ledgerNode.equals(AVAILABLE_NODE)) {
            ledgerMcb.processResult(BKException.Code.OK, null, null);
            return;
        }
        // Parse out the ledgerId from the ZK ledger node.
        String parts[] = ledgerNode.split(LEDGER_NODE_PREFIX);
        if (parts.length < 2) {
            LOG.error("Ledger Node retrieved from ZK has invalid name format: " + ledgerNode);
            ledgerMcb.processResult(BKException.Code.ZKException, null, null);
            return;
        }
        final long lId;
        try {
            lId = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            LOG.error("Error retrieving ledgerId from ledgerNode: " + ledgerNode, e);
            ledgerMcb.processResult(BKException.Code.ZKException, null, null);
            return;
        }
        /*
         * For the current ledger, open it to retrieve the LedgerHandle. This
         * will contain the LedgerMetadata indicating which bookie servers the
         * ledger fragments are stored on. Check if any of the ledger fragments
         * for the current ledger are stored on the input dead bookie.
         */
        DigestType digestType = getLedgerDigestType(lId);
        byte[] passwd = getLedgerPasswd(lId);
        bkc.asyncOpenLedgerNoRecovery(lId, digestType, passwd, new OpenCallback() {
            @Override
            public void openComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("BK error opening ledger: " + lId, BKException.create(rc));
                    ledgerMcb.processResult(rc, null, null);
                    return;
                }
                /*
                 * This List stores the ledger fragments to recover indexed by
                 * the start entry ID for the range. The ensembles TreeMap is
                 * keyed off this.
                 */
                final List<Long> ledgerFragmentsToRecover = new LinkedList<Long>();
                /*
                 * This Map will store the start and end entry ID values for
                 * each of the ledger fragment ranges. The only exception is the
                 * current active fragment since it has no end yet. In the event
                 * of a bookie failure, a new ensemble is created so the current
                 * ensemble should not contain the dead bookie we are trying to
                 * recover.
                 */
                Map<Long, Long> ledgerFragmentsRange = new HashMap<Long, Long>();
                Long curEntryId = null;
                for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : lh.getLedgerMetadata().getEnsembles()
                         .entrySet()) {
                    if (curEntryId != null)
                        ledgerFragmentsRange.put(curEntryId, entry.getKey() - 1);
                    curEntryId = entry.getKey();
                    if (entry.getValue().contains(bookieSrc)) {
                        /*
                         * Current ledger fragment has entries stored on the
                         * dead bookie so we'll need to recover them.
                         */
                        ledgerFragmentsToRecover.add(entry.getKey());
                    }
                }
                /*
                 * See if this current ledger contains any ledger fragment that
                 * needs to be re-replicated. If not, then just invoke the
                 * multiCallback and return.
                 */
                if (ledgerFragmentsToRecover.size() == 0) {
                    ledgerMcb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                /*
                 * Multicallback for ledger. Once all fragments for the ledger have been recovered
                 * trigger the ledgerMcb 
                 */
                MultiCallback ledgerFragmentsMcb 
                    = new MultiCallback(ledgerFragmentsToRecover.size(), ledgerMcb, null);

                /*
                 * Now recover all of the necessary ledger fragments
                 * asynchronously using a MultiCallback for every fragment.
                 */
                for (final Long startEntryId : ledgerFragmentsToRecover) {
                    Long endEntryId = ledgerFragmentsRange.get(startEntryId);
                    InetSocketAddress newBookie = null;
                    try {
                        newBookie = getNewBookie(lh.getLedgerMetadata().getEnsembles().get(startEntryId),
                                                 availableBookies);
                    } catch (BKException.BKNotEnoughBookiesException bke) {
                        ledgerFragmentsMcb.processResult(BKException.Code.NotEnoughBookiesException, 
                                                         null, null);
                        continue;
                    }
                    
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Replicating fragment from [" + startEntryId 
                                  + "," + endEntryId + "] of ledger " + lh.getId()
                                  + " to " + newBookie);
                    }

                    try {
                        SingleFragmentCallback cb = new SingleFragmentCallback(
                                                                               ledgerFragmentsMcb, lh, startEntryId, bookieSrc, newBookie);
                        recoverLedgerFragment(bookieSrc, lh, startEntryId, endEntryId, cb, newBookie);
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            }, null);
    }

    /**
     * This method asynchronously recovers a ledger fragment which is a
     * contiguous portion of a ledger that was stored in an ensemble that
     * included the failed bookie.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lh
     *            LedgerHandle for the ledger
     * @param startEntryId
     *            Start entry Id for the ledger fragment
     * @param endEntryId
     *            End entry Id for the ledger fragment
     * @param ledgerFragmentMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragment(final InetSocketAddress bookieSrc, final LedgerHandle lh,
                                       final Long startEntryId, final Long endEntryId, final SingleFragmentCallback cb,
                                       final InetSocketAddress newBookie) throws InterruptedException {
        if (endEntryId == null) {
            /*
             * Ideally this should never happen if bookie failure is taken care
             * of properly. Nothing we can do though in this case.
             */
            LOG.warn("Dead bookie (" + bookieSrc + ") is still part of the current active ensemble for ledgerId: "
                     + lh.getId());
            cb.processResult(BKException.Code.OK, null, null);
            return;
        }

        ArrayList<InetSocketAddress> curEnsemble = lh.getLedgerMetadata().getEnsembles().get(startEntryId);
        int bookieIndex = 0;
        for (int i = 0; i < curEnsemble.size(); i++) {
            if (curEnsemble.get(i).equals(bookieSrc)) {
                bookieIndex = i;
                break;
            }
        }
        /*
         * Loop through all entries in the current ledger fragment range and
         * find the ones that were stored on the dead bookie.
         */
        List<Long> entriesToReplicate = new LinkedList<Long>();
        for (long i = startEntryId; i <= endEntryId; i++) {
            if (lh.getDistributionSchedule().getReplicaIndex(i, bookieIndex) >= 0) {
                /*
                 * Current entry is stored on the dead bookie so we'll need to
                 * read it and replicate it to a new bookie.
                 */
                entriesToReplicate.add(i);
            }
        }
        /*
         * Now asynchronously replicate all of the entries for the ledger
         * fragment that were on the dead bookie.
         */
        MultiCallback ledgerFragmentEntryMcb = new MultiCallback(entriesToReplicate.size(), cb, null);
        for (final Long entryId : entriesToReplicate) {
            recoverLedgerFragmentEntry(entryId, lh, ledgerFragmentEntryMcb, newBookie);
        }
    }

    /**
     * This method asynchronously recovers a specific ledger entry by reading
     * the values via the BookKeeper Client (which would read it from the other
     * replicas) and then writing it to the chosen new bookie.
     *
     * @param entryId
     *            Ledger Entry ID to recover.
     * @param lh
     *            LedgerHandle for the ledger
     * @param ledgerFragmentEntryMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger entry.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragmentEntry(final Long entryId, final LedgerHandle lh,
                                            final MultiCallback ledgerFragmentEntryMcb, 
                                            final InetSocketAddress newBookie) throws InterruptedException {
        /*
         * Read the ledger entry using the LedgerHandle. This will allow us to
         * read the entry from one of the other replicated bookies other than
         * the dead one.
         */
        lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("BK error reading ledger entry: " + entryId, BKException.create(rc));
                    ledgerFragmentEntryMcb.processResult(rc, null, null);
                    return;
                }
                /*
                 * Now that we've read the ledger entry, write it to the new
                 * bookie we've selected.
                 */
                LedgerEntry entry = seq.nextElement();
                byte[] data = entry.getEntry();
                ChannelBuffer toSend = lh.getDigestManager().computeDigestAndPackageForSending(entryId,
                                       lh.getLastAddConfirmed(), entry.getLength(), data, 0, data.length);
                bkc.getBookieClient().addEntry(newBookie, lh.getId(), lh.getLedgerKey(), entryId, toSend,
                new WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr,
                    Object ctx) {
                        if (rc != Code.OK.intValue()) {
                            LOG.error("BK error writing entry for ledgerId: " + ledgerId + ", entryId: "
                                      + entryId + ", bookie: " + addr, BKException.create(rc));
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Success writing ledger id " +ledgerId + ", entry id "
                                          + entryId + " to a new bookie " + addr + "!");
                            }
                        }
                        /*
                         * Pass the return code result up the chain with
                         * the parent callback.
                         */
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                }, null, BookieProtocol.FLAG_RECOVERY_ADD);
            }
        }, null);
    }

    /*
     * Callback for recovery of a single ledger fragment.
     * Once the fragment has had all entries replicated, update the ensemble 
     * in zookeeper.
     * Once finished propogate callback up to ledgerFragmentsMcb which should
     * be a multicallback responsible for all fragments in a single ledger
     */
    class SingleFragmentCallback implements AsyncCallback.VoidCallback {
        final MultiCallback ledgerFragmentsMcb;
        final LedgerHandle lh;
        final long fragmentStartId;
        final InetSocketAddress oldBookie;
        final InetSocketAddress newBookie;

        SingleFragmentCallback(MultiCallback ledgerFragmentsMcb, LedgerHandle lh, 
                               long fragmentStartId,
                               InetSocketAddress oldBookie,
                               InetSocketAddress newBookie) {
            this.ledgerFragmentsMcb = ledgerFragmentsMcb;
            this.lh = lh;
            this.fragmentStartId = fragmentStartId;
            this.newBookie = newBookie;
            this.oldBookie = oldBookie;
        }
        
        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != Code.OK.intValue()) {
                LOG.error("BK error replicating ledger fragments for ledger: " + lh.getId(), 
                          BKException.create(rc));
                ledgerFragmentsMcb.processResult(rc, null, null);
                return;
            }
            /*
             * Update the ledger metadata's ensemble info to point
             * to the new bookie.
             */
            ArrayList<InetSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().get(
                    fragmentStartId);
            int deadBookieIndex = ensemble.indexOf(oldBookie);
            ensemble.remove(deadBookieIndex);
            ensemble.add(deadBookieIndex, newBookie);
            
            
            lh.writeLedgerConfig(new WriteCb(), null);
        }
        
        private class WriteCb implements AsyncCallback.StatCallback {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == Code.BADVERSION.intValue()) {
                    LOG.warn("Two fragments attempted update at once; ledger id: " + lh.getId() 
                             + " startid: " + fragmentStartId);
                    // try again, the previous success (with which this has conflicted)
                    // will have updated the stat
                    lh.writeLedgerConfig(new WriteCb(), null);
                    return;
                } else if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error updating ledger config metadata for ledgerId: " + lh.getId(),
                              KeeperException.create(KeeperException.Code.get(rc), path));
                } else {
                    lh.getLedgerMetadata().updateZnodeStatus(stat);
                    LOG.info("Updated ZK for ledgerId: (" + lh.getId() + " : " + fragmentStartId 
                             + ") to point ledger fragments from old dead bookie: (" + oldBookie
                             + ") to new bookie: (" + newBookie + ")");
                }
                /*
                 * Pass the return code result up the chain with
                 * the parent callback.
                 */
                ledgerFragmentsMcb.processResult(rc, null, null);
            }
        };
    }
}
