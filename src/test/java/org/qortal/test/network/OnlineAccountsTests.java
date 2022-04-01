package org.qortal.test.network;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.junit.Before;
import org.junit.Test;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.block.Block;
import org.qortal.block.BlockChain;
import org.qortal.controller.BlockMinter;
import org.qortal.controller.OnlineAccountsManager;
import org.qortal.data.network.OnlineAccountData;
import org.qortal.network.message.*;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryManager;
import org.qortal.settings.Settings;
import org.qortal.test.common.Common;
import org.qortal.transform.Transformer;
import org.qortal.utils.Base58;
import org.qortal.utils.NTP;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class OnlineAccountsTests extends Common {

    private static final Random RANDOM = new Random();
    static {
        // This must go before any calls to LogManager/Logger
        System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");

        Security.insertProviderAt(new BouncyCastleProvider(), 0);
        Security.insertProviderAt(new BouncyCastleJsseProvider(), 1);
    }

    @Before
    public void beforeTest() throws DataException, IOException {
        Common.useSettingsAndDb(Common.testSettingsFilename, false);
        NTP.setFixedOffset(Settings.getInstance().getTestNtpOffset());
    }


    @Test
    public void testGetOnlineAccountsV2() throws Message.MessageException {
        List<OnlineAccountData> onlineAccountsOut = generateOnlineAccounts(false);

        Message messageOut = new GetOnlineAccountsV2Message(onlineAccountsOut);

        byte[] messageBytes = messageOut.toBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(messageBytes);

        GetOnlineAccountsV2Message messageIn = (GetOnlineAccountsV2Message) Message.fromByteBuffer(byteBuffer);

        List<OnlineAccountData> onlineAccountsIn = messageIn.getOnlineAccounts();

        assertEquals("size mismatch", onlineAccountsOut.size(), onlineAccountsIn.size());
        assertTrue("accounts mismatch", onlineAccountsIn.containsAll(onlineAccountsOut));
    }

    @Test
    public void testOnlineAccountsV2() throws Message.MessageException {
        List<OnlineAccountData> onlineAccountsOut = generateOnlineAccounts(true);

        Message messageOut = new OnlineAccountsV2Message(onlineAccountsOut);

        byte[] messageBytes = messageOut.toBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(messageBytes);

        OnlineAccountsV2Message messageIn = (OnlineAccountsV2Message) Message.fromByteBuffer(byteBuffer);

        List<OnlineAccountData> onlineAccountsIn = messageIn.getOnlineAccounts();

        assertEquals("size mismatch", onlineAccountsOut.size(), onlineAccountsIn.size());
        assertTrue("accounts mismatch", onlineAccountsIn.containsAll(onlineAccountsOut));
    }

    private List<OnlineAccountData> generateOnlineAccounts(boolean withSignatures) {
        List<OnlineAccountData> onlineAccounts = new ArrayList<>();

        int numTimestamps = RANDOM.nextInt(2) + 1; // 1 or 2

        for (int t = 0; t < numTimestamps; ++t) {
            int numAccounts = RANDOM.nextInt(3000);

            for (int a = 0; a < numAccounts; ++a) {
                byte[] sig = null;
                if (withSignatures) {
                    sig = new byte[Transformer.SIGNATURE_LENGTH];
                    RANDOM.nextBytes(sig);
                }

                byte[] pubkey = new byte[Transformer.PUBLIC_KEY_LENGTH];
                RANDOM.nextBytes(pubkey);

                onlineAccounts.add(new OnlineAccountData(t << 32, sig, pubkey));
            }
        }

        return onlineAccounts;
    }

    @Test
    public void testOnlineAccountsModulusV1() throws IllegalAccessException, DataException {
        try (final Repository repository = RepositoryManager.getRepository()) {

            // Set feature trigger timestamp to MAX long so that it is inactive
            FieldUtils.writeField(BlockChain.getInstance(), "onlineAccountsModulusV2Timestamp", Long.MAX_VALUE, true);

            List<String> onlineAccountSignatures = new ArrayList<>();
            long fakeNTPOffset = 0L;

            // Mint a block and store its timestamp
            Block block = BlockMinter.mintTestingBlock(repository, Common.getTestAccount(repository, "alice-reward-share"));
            long lastBlockTimestamp = block.getBlockData().getTimestamp();

            // Mint some blocks and keep track of the different online account signatures
            for (int i = 0; i < 30; i++) {
                block = BlockMinter.mintTestingBlock(repository, Common.getTestAccount(repository, "alice-reward-share"));

                // Increase NTP fixed offset by the block time, to simulate time passing
                long blockTimeDelta = block.getBlockData().getTimestamp() - lastBlockTimestamp;
                lastBlockTimestamp = block.getBlockData().getTimestamp();
                fakeNTPOffset += blockTimeDelta;
                NTP.setFixedOffset(fakeNTPOffset);

                String lastOnlineAccountSignatures58 = Base58.encode(block.getBlockData().getOnlineAccountsSignatures());
                if (!onlineAccountSignatures.contains(lastOnlineAccountSignatures58)) {
                    onlineAccountSignatures.add(lastOnlineAccountSignatures58);
                }
            }

            // We expect at least 6 unique signatures over 30 blocks (generally 6-8, but could be higher due to block time differences)
            System.out.println(String.format("onlineAccountSignatures count: %d", onlineAccountSignatures.size()));
            assertTrue(onlineAccountSignatures.size() >= 6);
        }
    }

    @Test
    public void testOnlineAccountsModulusV2() throws IllegalAccessException, DataException {
        try (final Repository repository = RepositoryManager.getRepository()) {

            // Set feature trigger timestamp to 0 so that it is active
            FieldUtils.writeField(BlockChain.getInstance(), "onlineAccountsModulusV2Timestamp", 0L, true);

            List<String> onlineAccountSignatures = new ArrayList<>();
            long fakeNTPOffset = 0L;

            // Mint a block and store its timestamp
            Block block = BlockMinter.mintTestingBlock(repository, Common.getTestAccount(repository, "alice-reward-share"));
            long lastBlockTimestamp = block.getBlockData().getTimestamp();

            // Mint some blocks and keep track of the different online account signatures
            for (int i = 0; i < 30; i++) {
                block = BlockMinter.mintTestingBlock(repository, Common.getTestAccount(repository, "alice-reward-share"));

                // Increase NTP fixed offset by the block time, to simulate time passing
                long blockTimeDelta = block.getBlockData().getTimestamp() - lastBlockTimestamp;
                lastBlockTimestamp = block.getBlockData().getTimestamp();
                fakeNTPOffset += blockTimeDelta;
                NTP.setFixedOffset(fakeNTPOffset);

                String lastOnlineAccountSignatures58 = Base58.encode(block.getBlockData().getOnlineAccountsSignatures());
                if (!onlineAccountSignatures.contains(lastOnlineAccountSignatures58)) {
                    onlineAccountSignatures.add(lastOnlineAccountSignatures58);
                }
            }

            // We expect 1-3 unique signatures over 30 blocks
            System.out.println(String.format("onlineAccountSignatures count: %d", onlineAccountSignatures.size()));
            assertTrue(onlineAccountSignatures.size() >= 1 && onlineAccountSignatures.size() <= 3);
        }
    }
}
