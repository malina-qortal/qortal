package org.qortal.transform.block;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.qortal.block.Block;
import org.qortal.block.BlockChain;
import org.qortal.data.at.ATStateData;
import org.qortal.data.block.BlockData;
import org.qortal.data.transaction.TransactionData;
import org.qortal.repository.DataException;
import org.qortal.transaction.Transaction;
import org.qortal.transaction.Transaction.TransactionType;
import org.qortal.transform.TransformationException;
import org.qortal.transform.Transformer;
import org.qortal.transform.transaction.TransactionTransformer;
import org.qortal.utils.Base58;
import org.qortal.utils.Serialization;
import org.qortal.utils.Triple;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import io.druid.extendedset.intset.ConciseSet;

public class BlockTransformer extends Transformer {

	private static final int VERSION_LENGTH = INT_LENGTH;
	private static final int TRANSACTIONS_SIGNATURE_LENGTH = SIGNATURE_LENGTH;
	private static final int MINTER_SIGNATURE_LENGTH = SIGNATURE_LENGTH;
	private static final int BLOCK_REFERENCE_LENGTH = MINTER_SIGNATURE_LENGTH + TRANSACTIONS_SIGNATURE_LENGTH;
	private static final int MINTER_PUBLIC_KEY_LENGTH = PUBLIC_KEY_LENGTH;
	private static final int TRANSACTION_COUNT_LENGTH = INT_LENGTH;

	private static final int BASE_LENGTH = VERSION_LENGTH + TIMESTAMP_LENGTH + BLOCK_REFERENCE_LENGTH + MINTER_PUBLIC_KEY_LENGTH
			+ TRANSACTIONS_SIGNATURE_LENGTH + MINTER_SIGNATURE_LENGTH + TRANSACTION_COUNT_LENGTH;

	public static final int BLOCK_SIGNATURE_LENGTH = MINTER_SIGNATURE_LENGTH + TRANSACTIONS_SIGNATURE_LENGTH;

	protected static final int TRANSACTION_SIZE_LENGTH = INT_LENGTH; // per transaction

	protected static final int AT_BYTES_LENGTH = INT_LENGTH;
	protected static final int AT_FEES_LENGTH = AMOUNT_LENGTH;
	protected static final int AT_LENGTH = AT_FEES_LENGTH + AT_BYTES_LENGTH;

	protected static final int ONLINE_ACCOUNTS_COUNT_LENGTH = INT_LENGTH;
	protected static final int ONLINE_ACCOUNTS_SIZE_LENGTH = INT_LENGTH;
	protected static final int ONLINE_ACCOUNTS_TIMESTAMP_LENGTH = TIMESTAMP_LENGTH;
	protected static final int ONLINE_ACCOUNTS_SIGNATURES_COUNT_LENGTH = INT_LENGTH;

	protected static final int AT_ENTRY_LENGTH = ADDRESS_LENGTH + SHA256_LENGTH + AMOUNT_LENGTH;

	/**
	 * Extract block data and transaction data from serialized bytes.
	 * 
	 * @param bytes
	 * @return BlockData and a List of transactions.
	 * @throws TransformationException
	 */
	public static Triple<BlockData, List<TransactionData>, List<ATStateData>> fromBytes(byte[] bytes) throws TransformationException {
		if (bytes == null)
			return null;

		if (bytes.length < BASE_LENGTH)
			throw new TransformationException("Byte data too short for Block");

		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

		return fromByteBuffer(byteBuffer);
	}

	/**
	 * Extract block data and transaction data from serialized bytes containing a single block.
	 *
	 * @param bytes
	 * @return BlockData and a List of transactions.
	 * @throws TransformationException
	 */
	public static Triple<BlockData, List<TransactionData>, List<ATStateData>> fromByteBuffer(ByteBuffer byteBuffer) throws TransformationException {
		return BlockTransformer.fromByteBuffer(byteBuffer, true);
	}

	/**
	 * Extract block data and transaction data from serialized bytes containing one or more blocks.
	 * 
	 * @param bytes
	 * @return the next block's BlockData and a List of transactions.
	 * @throws TransformationException
	 */
	public static Triple<BlockData, List<TransactionData>, List<ATStateData>> fromByteBuffer(ByteBuffer byteBuffer, boolean finalBlockInBuffer) throws TransformationException {
		int version = byteBuffer.getInt();

		if (finalBlockInBuffer && byteBuffer.remaining() < BASE_LENGTH + AT_BYTES_LENGTH - VERSION_LENGTH)
			throw new TransformationException("Byte data too short for Block");

		if (finalBlockInBuffer && byteBuffer.remaining() > BlockChain.getInstance().getMaxBlockSize())
			throw new TransformationException("Byte data too long for Block");

		long timestamp = byteBuffer.getLong();

		byte[] reference = new byte[BLOCK_REFERENCE_LENGTH];
		byteBuffer.get(reference);

		byte[] minterPublicKey = Serialization.deserializePublicKey(byteBuffer);

		byte[] transactionsSignature = new byte[TRANSACTIONS_SIGNATURE_LENGTH];
		byteBuffer.get(transactionsSignature);

		byte[] minterSignature = new byte[MINTER_SIGNATURE_LENGTH];
		byteBuffer.get(minterSignature);

		long totalFees = 0;

		int atCount = 0;
		long atFees = 0;
		List<ATStateData> atStates = new ArrayList<>();

		int atBytesLength = byteBuffer.getInt();

		if (atBytesLength > BlockChain.getInstance().getMaxBlockSize())
			throw new TransformationException("Byte data too long for Block's AT info");

		ByteBuffer atByteBuffer = byteBuffer.slice();
		atByteBuffer.limit(atBytesLength);

		// Read AT-address, SHA256 hash and fees
		if (atBytesLength % AT_ENTRY_LENGTH != 0)
			throw new TransformationException("AT byte data not a multiple of AT entry length");

		while (atByteBuffer.hasRemaining()) {
			byte[] atAddressBytes = new byte[ADDRESS_LENGTH];
			atByteBuffer.get(atAddressBytes);
			String atAddress = Base58.encode(atAddressBytes);

			byte[] stateHash = new byte[SHA256_LENGTH];
			atByteBuffer.get(stateHash);

			long fees = atByteBuffer.getLong();

			// Add this AT's fees to our total
			atFees += fees;

			atStates.add(new ATStateData(atAddress, stateHash, fees));
		}

		// Bump byteBuffer over AT states just read in slice
		byteBuffer.position(byteBuffer.position() + atBytesLength);

		// AT count to reflect the number of states we have
		atCount = atStates.size();

		// Add AT fees to totalFees
		totalFees += atFees;

		int transactionCount = byteBuffer.getInt();

		// Parse transactions now, compared to deferred parsing in Gen1, so we can throw ParseException if need be.
		List<TransactionData> transactions = new ArrayList<>();

		for (int t = 0; t < transactionCount; ++t) {
			if (byteBuffer.remaining() < TRANSACTION_SIZE_LENGTH)
				throw new TransformationException("Byte data too short for Block Transaction length");

			int transactionLength = byteBuffer.getInt();

			if (byteBuffer.remaining() < transactionLength)
				throw new TransformationException("Byte data too short for Block Transaction");

			if (transactionLength > BlockChain.getInstance().getMaxBlockSize())
				throw new TransformationException("Byte data too long for Block Transaction");

			byte[] transactionBytes = new byte[transactionLength];
			byteBuffer.get(transactionBytes);

			TransactionData transactionData = TransactionTransformer.fromBytes(transactionBytes);
			transactions.add(transactionData);

			totalFees += transactionData.getFee();
		}

		// Online accounts info?
		byte[] encodedOnlineAccounts = null;
		int onlineAccountsCount = 0;
		byte[] onlineAccountsSignatures = null;
		Long onlineAccountsTimestamp = null;

		onlineAccountsCount = byteBuffer.getInt();

		int conciseSetLength = byteBuffer.getInt();

		if (conciseSetLength > BlockChain.getInstance().getMaxBlockSize())
			throw new TransformationException("Byte data too long for online account info");

		if ((conciseSetLength & 3) != 0)
			throw new TransformationException("Byte data length not multiple of 4 for online account info");

		encodedOnlineAccounts = new byte[conciseSetLength];
		byteBuffer.get(encodedOnlineAccounts);

		// Try to decode to ConciseSet
		ConciseSet accountsIndexes = BlockTransformer.decodeOnlineAccounts(encodedOnlineAccounts);
		if (accountsIndexes.size() != onlineAccountsCount)
			throw new TransformationException("Block's online account data malformed");

		// Note: number of signatures, not byte length
		int onlineAccountsSignaturesCount = byteBuffer.getInt();

		if (onlineAccountsSignaturesCount > 0) {
			// Online accounts timestamp is only present if there are also signatures
			onlineAccountsTimestamp = byteBuffer.getLong();

			final int signaturesByteLength = onlineAccountsSignaturesCount * Transformer.SIGNATURE_LENGTH;
			if (signaturesByteLength > BlockChain.getInstance().getMaxBlockSize())
				throw new TransformationException("Byte data too long for online accounts signatures");

			onlineAccountsSignatures = new byte[signaturesByteLength];
			byteBuffer.get(onlineAccountsSignatures);
		}

		// We should only complain about excess byte data if we aren't expecting more blocks in this ByteBuffer
		if (finalBlockInBuffer && byteBuffer.hasRemaining())
			throw new TransformationException("Excess byte data found after parsing Block");

		// We don't have a height!
		Integer height = null;
		BlockData blockData = new BlockData(version, reference, transactionCount, totalFees, transactionsSignature, height, timestamp,
				minterPublicKey, minterSignature, atCount, atFees, encodedOnlineAccounts, onlineAccountsCount, onlineAccountsTimestamp, onlineAccountsSignatures);

		return new Triple<>(blockData, transactions, atStates);
	}

	public static int getDataLength(Block block) throws TransformationException {
		BlockData blockData = block.getBlockData();
		int blockLength = BASE_LENGTH;

		blockLength += AT_BYTES_LENGTH + blockData.getATCount() * AT_ENTRY_LENGTH;
		blockLength += ONLINE_ACCOUNTS_COUNT_LENGTH + ONLINE_ACCOUNTS_SIZE_LENGTH + blockData.getEncodedOnlineAccounts().length;
		blockLength += ONLINE_ACCOUNTS_SIGNATURES_COUNT_LENGTH;

		byte[] onlineAccountsSignatures = blockData.getOnlineAccountsSignatures();
		if (onlineAccountsSignatures != null && onlineAccountsSignatures.length > 0)
			blockLength += ONLINE_ACCOUNTS_TIMESTAMP_LENGTH + blockData.getOnlineAccountsSignatures().length;

		try {
			// Short cut for no transactions
			List<Transaction> transactions = block.getTransactions();
			if (transactions == null || transactions.isEmpty())
				return blockLength;

			for (Transaction transaction : transactions) {
				// We don't serialize AT transactions
				if (transaction.getTransactionData().getType() == TransactionType.AT)
					continue;

				blockLength += TRANSACTION_SIZE_LENGTH + TransactionTransformer.getDataLength(transaction.getTransactionData());
			}
		} catch (DataException e) {
			throw new TransformationException("Unable to determine serialized block length", e);
		}

		return blockLength;
	}

	public static byte[] toBytes(Block block) throws TransformationException {
		BlockData blockData = block.getBlockData();

		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream(getDataLength(block));

			bytes.write(Ints.toByteArray(blockData.getVersion()));
			bytes.write(Longs.toByteArray(blockData.getTimestamp()));
			bytes.write(blockData.getReference());
			bytes.write(blockData.getMinterPublicKey());
			bytes.write(blockData.getTransactionsSignature());
			bytes.write(blockData.getMinterSignature());

			int atBytesLength = blockData.getATCount() * AT_ENTRY_LENGTH;
			bytes.write(Ints.toByteArray(atBytesLength));

			for (ATStateData atStateData : block.getATStates()) {
				// Skip initial states generated by DEPLOY_AT transactions in the same block
				if (atStateData.isInitial())
					continue;

				bytes.write(Base58.decode(atStateData.getATAddress()));
				bytes.write(atStateData.getStateHash());
				bytes.write(Longs.toByteArray(atStateData.getFees()));
			}

			// Transactions
			bytes.write(Ints.toByteArray(blockData.getTransactionCount()));

			for (Transaction transaction : block.getTransactions()) {
				// Don't serialize AT transactions!
				if (transaction.getTransactionData().getType() == TransactionType.AT)
					continue;

				TransactionData transactionData = transaction.getTransactionData();
				bytes.write(Ints.toByteArray(TransactionTransformer.getDataLength(transactionData)));
				bytes.write(TransactionTransformer.toBytes(transactionData));
			}

			// Online account info
			byte[] encodedOnlineAccounts = blockData.getEncodedOnlineAccounts();

			if (encodedOnlineAccounts != null) {
				bytes.write(Ints.toByteArray(blockData.getOnlineAccountsCount()));

				bytes.write(Ints.toByteArray(encodedOnlineAccounts.length));
				bytes.write(encodedOnlineAccounts);
			} else {
				bytes.write(Ints.toByteArray(0)); // onlineAccountsCount
				bytes.write(Ints.toByteArray(0)); // encodedOnlineAccounts length
			}

			byte[] onlineAccountsSignatures = blockData.getOnlineAccountsSignatures();

			if (onlineAccountsSignatures != null && onlineAccountsSignatures.length > 0) {
				// Note: we write the number of signatures, not the number of bytes
				bytes.write(Ints.toByteArray(onlineAccountsSignatures.length / Transformer.SIGNATURE_LENGTH));

				// We only write online accounts timestamp if we have signatures
				bytes.write(Longs.toByteArray(blockData.getOnlineAccountsTimestamp()));

				bytes.write(onlineAccountsSignatures);
			} else {
				// Zero online accounts signatures (timestamp omitted also)
				bytes.write(Ints.toByteArray(0));
			}

			return bytes.toByteArray();
		} catch (IOException | DataException e) {
			throw new TransformationException("Unable to serialize block", e);
		}
	}

	private static byte[] getReferenceBytesForMinterSignature(int blockHeight, byte[] reference) {
		int newBlockSigTriggerHeight = BlockChain.getInstance().getNewBlockSigHeight();

		return blockHeight >= newBlockSigTriggerHeight
				// 'new' block sig uses all of previous block's signature
				? reference
				// 'old' block sig only uses first 64 bytes of previous block's signature
				: Arrays.copyOf(reference, MINTER_SIGNATURE_LENGTH);
	}

	public static byte[] getBytesForMinterSignature(BlockData blockData) {
		byte[] referenceBytes = getReferenceBytesForMinterSignature(blockData.getHeight(), blockData.getReference());

		return getBytesForMinterSignature(referenceBytes, blockData.getMinterPublicKey(), blockData.getEncodedOnlineAccounts());
	}

	public static byte[] getBytesForMinterSignature(BlockData parentBlockData, byte[] minterPublicKey, byte[] encodedOnlineAccounts) {
		byte[] referenceBytes = getReferenceBytesForMinterSignature(parentBlockData.getHeight() + 1, parentBlockData.getSignature());

		return getBytesForMinterSignature(referenceBytes, minterPublicKey, encodedOnlineAccounts);
	}

	private static byte[] getBytesForMinterSignature(byte[] referenceBytes, byte[] minterPublicKey, byte[] encodedOnlineAccounts) {
		byte[] bytes = new byte[referenceBytes.length + MINTER_PUBLIC_KEY_LENGTH + encodedOnlineAccounts.length];

		System.arraycopy(referenceBytes, 0, bytes, 0, referenceBytes.length);

		System.arraycopy(minterPublicKey, 0, bytes, referenceBytes.length, MINTER_PUBLIC_KEY_LENGTH);

		System.arraycopy(encodedOnlineAccounts, 0, bytes, referenceBytes.length + MINTER_PUBLIC_KEY_LENGTH, encodedOnlineAccounts.length);

		return bytes;
	}

	public static byte[] getBytesForTransactionsSignature(Block block) throws TransformationException {
		try {
			List<Transaction> transactions = block.getTransactions();

			ByteArrayOutputStream bytes = new ByteArrayOutputStream(MINTER_SIGNATURE_LENGTH + transactions.size() * TransactionTransformer.SIGNATURE_LENGTH);

			bytes.write(block.getBlockData().getMinterSignature());

			for (Transaction transaction : transactions) {
				// We don't include AT-Transactions as AT-state/output is dealt with elsewhere in the block code
				if (transaction.getTransactionData().getType() == TransactionType.AT)
					continue;

				if (!transaction.isSignatureValid())
					throw new TransformationException("Transaction signature invalid when building block's transactions signature");

				bytes.write(transaction.getTransactionData().getSignature());
			}

			return bytes.toByteArray();
		} catch (IOException | DataException e) {
			throw new TransformationException(e);
		}
	}

	public static byte[] encodeOnlineAccounts(ConciseSet onlineAccounts) {
		return onlineAccounts.toByteBuffer().array();
	}

	public static ConciseSet decodeOnlineAccounts(byte[] encodedOnlineAccounts) {
		int[] words = new int[encodedOnlineAccounts.length / 4];
		ByteBuffer.wrap(encodedOnlineAccounts).asIntBuffer().get(words);
		return new ConciseSet(words, false);
	}

	public static byte[] encodeTimestampSignatures(List<byte[]> signatures) {
		byte[] encodedSignatures = new byte[signatures.size() * Transformer.SIGNATURE_LENGTH];

		for (int i = 0; i < signatures.size(); ++i)
			System.arraycopy(signatures.get(i), 0, encodedSignatures, i * Transformer.SIGNATURE_LENGTH, Transformer.SIGNATURE_LENGTH);

		return encodedSignatures;
	}

	public static List<byte[]> decodeTimestampSignatures(byte[] encodedSignatures) {
		List<byte[]> signatures = new ArrayList<>();

		for (int i = 0; i < encodedSignatures.length; i += Transformer.SIGNATURE_LENGTH) {
			byte[] signature = new byte[Transformer.SIGNATURE_LENGTH];
			System.arraycopy(encodedSignatures, i, signature, 0, Transformer.SIGNATURE_LENGTH);
			signatures.add(signature);
		}

		return signatures;
	}

}
