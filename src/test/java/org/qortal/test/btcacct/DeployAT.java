package org.qortal.test.btcacct;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.qortal.account.PrivateKeyAccount;
import org.qortal.asset.Asset;
import org.qortal.crosschain.BTCACCT;
import org.qortal.data.transaction.BaseTransactionData;
import org.qortal.data.transaction.DeployAtTransactionData;
import org.qortal.data.transaction.TransactionData;
import org.qortal.group.Group;
import org.qortal.repository.DataException;
import org.qortal.repository.Repository;
import org.qortal.repository.RepositoryFactory;
import org.qortal.repository.RepositoryManager;
import org.qortal.repository.hsqldb.HSQLDBRepositoryFactory;
import org.qortal.settings.Settings;
import org.qortal.transaction.DeployAtTransaction;
import org.qortal.transaction.Transaction;
import org.qortal.transform.TransformationException;
import org.qortal.transform.transaction.TransactionTransformer;
import org.qortal.utils.Amounts;
import org.qortal.utils.Base58;

import com.google.common.hash.HashCode;

public class DeployAT {

	public static final long atFundingExtra = 2000000L;

	private static void usage(String error) {
		if (error != null)
			System.err.println(error);

		System.err.println(String.format("usage: DeployAT <your Qortal PRIVATE key> <QORT amount> <BTC amount> <your Bitcoin PKH> <HASH160-of-secret> <AT funding amount> <trade-timeout>"));
		System.err.println(String.format("example: DeployAT "
				+ "AdTd9SUEYSdTW8mgK3Gu72K97bCHGdUwi2VvLNjUohot \\\n"
				+ "\t80.4020 \\\n"
				+ "\t0.00864200 \\\n"
				+ "\t750b06757a2448b8a4abebaa6e4662833fd5ddbb \\\n"
				+ "\tdaf59884b4d1aec8c1b17102530909ee43c0151a \\\n"
				+ "\t123.456 \\\n"
				+ "\t10080"));
		System.exit(1);
	}

	public static void main(String[] args) {
		if (args.length != 7)
			usage(null);

		Security.insertProviderAt(new BouncyCastleProvider(), 0);
		Settings.fileInstance("settings-test.json");

		byte[] refundPrivateKey = null;
		long redeemAmount = 0;
		long expectedBitcoin = 0;
		byte[] bitcoinPublicKeyHash = null;
		byte[] secretHash = null;
		long fundingAmount = 0;
		int tradeTimeout = 0;

		int argIndex = 0;
		try {
			refundPrivateKey = Base58.decode(args[argIndex++]);
			if (refundPrivateKey.length != 32)
				usage("Refund private key must be 32 bytes");

			redeemAmount = Long.parseLong(args[argIndex++]);
			if (redeemAmount <= 0)
				usage("QORT amount must be positive");

			expectedBitcoin = Long.parseLong(args[argIndex++]);
			if (expectedBitcoin <= 0)
				usage("Expected BTC amount must be positive");

			bitcoinPublicKeyHash = HashCode.fromString(args[argIndex++]).asBytes();
			if (bitcoinPublicKeyHash.length != 20)
				usage("Bitcoin PKH must be 20 bytes");

			secretHash = HashCode.fromString(args[argIndex++]).asBytes();
			if (secretHash.length != 20)
				usage("Hash of secret must be 20 bytes");

			fundingAmount = Long.parseLong(args[argIndex++]);
			if (fundingAmount <= redeemAmount)
				usage("AT funding amount must be greater than QORT redeem amount");

			tradeTimeout = Integer.parseInt(args[argIndex++]);
			if (tradeTimeout < 60 || tradeTimeout > 50000)
				usage("Trade timeout (minutes) must be between 60 and 50000");
		} catch (IllegalArgumentException e) {
			usage(String.format("Invalid argument %d: %s", argIndex, e.getMessage()));
		}

		try {
			RepositoryFactory repositoryFactory = new HSQLDBRepositoryFactory(Settings.getInstance().getRepositoryPath());
			RepositoryManager.setRepositoryFactory(repositoryFactory);
		} catch (DataException e) {
			throw new RuntimeException("Repository startup issue: " + e.getMessage());
		}

		try (final Repository repository = RepositoryManager.getRepository()) {
			System.out.println("Confirm the following is correct based on the info you've given:");

			PrivateKeyAccount refundAccount = new PrivateKeyAccount(repository, refundPrivateKey);
			System.out.println(String.format("Refund Qortal address: %s", refundAccount.getAddress()));

			System.out.println(String.format("QORT redeem amount: %s", Amounts.prettyAmount(redeemAmount)));

			System.out.println(String.format("AT funding amount: %s", Amounts.prettyAmount(fundingAmount)));

			System.out.println(String.format("HASH160 of secret: %s", HashCode.fromBytes(secretHash)));

			// Deploy AT
			byte[] creationBytes = BTCACCT.buildQortalAT(refundAccount.getAddress(), bitcoinPublicKeyHash, secretHash, redeemAmount, expectedBitcoin, tradeTimeout);
			System.out.println("CIYAM AT creation bytes: " + HashCode.fromBytes(creationBytes).toString());

			long txTimestamp = System.currentTimeMillis();
			byte[] lastReference = refundAccount.getLastReference();

			if (lastReference == null) {
				System.err.println(String.format("Qortal account %s has no last reference", refundAccount.getAddress()));
				System.exit(2);
			}

			Long fee = null;
			String name = "QORT-BTC cross-chain trade";
			String description = String.format("Qortal-Bitcoin cross-chain trade");
			String atType = "ACCT";
			String tags = "QORT-BTC ACCT";

			BaseTransactionData baseTransactionData = new BaseTransactionData(txTimestamp, Group.NO_GROUP, lastReference, refundAccount.getPublicKey(), fee, null);
			TransactionData deployAtTransactionData = new DeployAtTransactionData(baseTransactionData, name, description, atType, tags, creationBytes, fundingAmount, Asset.QORT);

			Transaction deployAtTransaction = new DeployAtTransaction(repository, deployAtTransactionData);

			fee = deployAtTransaction.calcRecommendedFee();
			deployAtTransactionData.setFee(fee);

			deployAtTransaction.sign(refundAccount);

			byte[] signedBytes = null;
			try {
				signedBytes = TransactionTransformer.toBytes(deployAtTransactionData);
			} catch (TransformationException e) {
				System.err.println(String.format("Unable to convert transaction to base58: %s", e.getMessage()));
				System.exit(2);
			}

			System.out.println(String.format("\nSigned transaction in base58, ready for POST /transactions/process:\n%s\n", Base58.encode(signedBytes)));
		} catch (NumberFormatException e) {
			usage(String.format("Number format exception: %s", e.getMessage()));
		} catch (DataException e) {
			throw new RuntimeException("Repository issue: " + e.getMessage());
		}
	}

}
