package com.song.fastmq.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.io.Charsets;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * Created by song on 上午11:47.
 */
public class BookKeeperTest {

    @Test
    public void createLedgers() throws Exception {
        try {
            BookKeeper bookKeeperClient = getKeeper();
            LedgerHandle ledger = bookKeeperClient.createLedger(BookKeeper.DigestType.MAC, "123456".getBytes(Charsets.UTF_8));
            long entryId = ledger.addEntry("Hello World".getBytes());
            System.out.println(entryId);
            Enumeration<LedgerEntry> enumeration = ledger.readEntries(0, 99);
            while (enumeration.hasMoreElements()) {
                LedgerEntry entry = enumeration.nextElement();
                System.out.println(entry.getEntryId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createLedgersAsync() throws Exception {
        BookKeeper keeper = getKeeper();
        CountDownLatch latch = new CountDownLatch(1);
        keeper.asyncCreateLedger(3, 2, BookKeeper.DigestType.MAC, "123456".getBytes(Charsets.UTF_8), (rc, lh, ctx) -> {
            System.out.println("Ledger successfully created");
            System.out.println(lh.getId());
            latch.countDown();
        }, "Some Context");
        latch.await();
    }

    @Test
    public void readEntry() throws Exception {
        BookKeeper keeper = getKeeper();
        LedgerHandle ledgerHandle = keeper.openLedger(16L, BookKeeper.DigestType.MAC, "123456".getBytes(Charsets.UTF_8));
        ledgerHandle.addEntry("Hello World".getBytes());
        Enumeration<LedgerEntry> enumeration = ledgerHandle.readEntries(1, 99);
        while (enumeration.hasMoreElements()) {
            LedgerEntry ledgerEntry = enumeration.nextElement();
            System.out.println(ledgerEntry.getEntryId());
        }
    }

    @Test
    public void name() throws Exception {
        // Create a client object for the local ensemble. This
        // operation throws multiple exceptions, so make sure to
        // use a try/catch block when instantiating client objects.
        BookKeeper bkc = new BookKeeper("localhost:2181");

        // A password for the new ledger
        byte[] ledgerPassword = "123456".getBytes();

        // Create a new ledger and fetch its identifier
        LedgerHandle lh = bkc.createLedger(BookKeeper.DigestType.MAC, ledgerPassword);
        long ledgerId = lh.getId();
        System.out.println("ledgerId: " + ledgerId);

        // Create a buffer for four-byte entries
        ByteBuffer entry = ByteBuffer.allocate(4);

        int numberOfEntries = 100;

        // Add entries to the ledger, then close it
        for (int i = 0; i < numberOfEntries; i++) {
            entry.putInt(i);
            entry.position(0);
            lh.addEntry(entry.array());
        }
        lh.close();

        // Open the ledger for reading
        lh = bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, ledgerPassword);

        // Read all available entries
        Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries-1);

        while (entries.hasMoreElements()) {
            ByteBuffer result = ByteBuffer.wrap(entries.nextElement().getEntry());
            Integer retrEntry = result.getInt();

            // Print the integer stored in each entry
            System.out.println(String.format("Result: %s", retrEntry));
        }

        // Close the ledger and the client
        lh.close();
        bkc.close();
    }

    private BookKeeper getKeeper() throws IOException, InterruptedException, KeeperException {
        String connectionString = "127.0.0.1:2181";
        return new BookKeeper(connectionString);
    }

}

