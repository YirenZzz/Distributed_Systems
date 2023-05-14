// package tester;
package app_kvServer;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.EOFException;

import org.apache.log4j.Logger;
import logger.LogSetup;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
public class FileDB {
	final int numShortValEntries = 9973; // file containing short values
	final int numLongValEntries = 997; // file containing long values
	final int cacheSize = 4999; // number of k-v pair in cache

	// file component size
	final int validByteLength = 1;
	final int keySizeLength = 4; // length of integer type
	final int maxKeySize = 20;
	final int entryValOffset = 25;
	final int valueSizeLength = 4;
	final int maxShortValueSize = 4096; // Length of short value entries
	final int maxLongValueSize = 122880; //120 kB
	final int maxShortValLen = 4096; // same as maxShortValueSize

	// final String filePathShortVals = "dbFileShortVal.txt";
	// final String filePathLongVals = "dbFileLongVal.txt";
	String filePathShortVals;
	String filePathLongVals;

	// offsets in file
	final int shortValEntryLength = 4125;
	final int longValEntryLength = 122909;

	// error codes;
	final int errSuccess = 0;
	final int errFailed = -1;
	final int errSizeLimitReached = -2;
	final int errEntryNotFound = -3;
	final int errFileFull = -4;

	final ReentrantReadWriteLock shortValFileLock = new ReentrantReadWriteLock();
	final ReentrantReadWriteLock longValFileLock = new ReentrantReadWriteLock();

	private Logger logger;
	/*
	 * constructor
	 */
	public FileDB(String path, String serverName){
        filePathShortVals = path + serverName + "_dbFileShortVal.txt";
		filePathLongVals = path + serverName + "_dbFileLongVal.txt";
		logger = Logger.getRootLogger();
    }

	/*
	 * Returns value of the key if key exists, and null if key does not exist.
	 * A thread-safe function
	 */
	public String get(String key) {

		File f = new File(filePathShortVals);
		RandomAccessFile raf;

		try {
			if (!f.isFile()) {
				logger.warn("Command: get " + key + ", file does not exist.");
				return null;
			}
			raf = new RandomAccessFile(filePathShortVals, "rw");
		} catch (IOException e) {
			logger.error("Error Opening File " + filePathShortVals);
			return null;
		}

		// read from short value file
		String shortFileRes = getFromFile(raf, key, true);
		if (shortFileRes != null) {
			return shortFileRes;
		}

		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error Closing File " + filePathShortVals);
		}

		// read from long value file if not found in shortValFile
		try {
			f = new File(filePathLongVals);
			if (!f.isFile()) {
				return null;
			}
			raf = new RandomAccessFile(filePathLongVals, "rw");
		} catch (IOException e) {
			logger.error("Error Opening File " + filePathLongVals);
			return null;
		}

		String res = getFromFile(raf, key, false);
		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error Closing File " + filePathLongVals);
		}
		return res;
	}

	/*
	 * Get value of a key from file
	 * 
	 */
	private String getFromFile(RandomAccessFile raf, String key, boolean isShortValFile) {
		ReentrantReadWriteLock fileLock = shortValFileLock;
		if (!isShortValFile) {
			fileLock = longValFileLock;
		}
		fileLock.readLock().lock();
		int res = getFileEntryIdx(raf, key, isShortValFile);

		if (res < 0) {
			fileLock.readLock().unlock();
			logger.warn("getFileEntryIdx failed" + res);
			return null;
		}

		String val = getFileEntryValByIdx(raf, res, isShortValFile);
		if (val == null) {
			logger.error("got null value from file entry");
		}
		fileLock.readLock().unlock();
		return val;
	}

	/*
	 * Insert key to file
	 * A thread-safe function
	 */
	public boolean put(String key, String val) {
		if (val == null || val == "" || val.equals("null")) {
			logger.info("In put function, type is delete");
			return del(key);
		}

		// Choose the file to save at based on the length of the string value
		String filePath = filePathShortVals;
		ReentrantReadWriteLock fileLock = shortValFileLock;
		boolean isShortValFile = true;
		if (val.getBytes().length > maxShortValLen) {
			filePath = filePathLongVals;
			fileLock = longValFileLock;
			isShortValFile = false;
		}

		File f = new File(filePath);
		RandomAccessFile raf;

		try {
			if (!f.isFile()) {
				f.createNewFile();
			}
			raf = new RandomAccessFile(filePath, "rw");
		} catch (IOException e) {
			logger.error("Error Creating File " + filePath);
			return false;
		}

		fileLock.writeLock().lock();

		// create or update entry
		int res = putFileEntry(raf, key, val, isShortValFile);
		fileLock.writeLock().unlock();

		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error Closing File " + filePath);
			return false;
		}

		return (res == errSuccess);
	}

	/*
	 * Delete key from both files
	 * 
	 */
	public boolean del(String key) {
		// Delete key from short value file
		File f = new File(filePathShortVals);
		if (!f.isFile()) {
			logger.error("del file not found");
			return true;
		}

		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(filePathShortVals, "rw");
		} catch (IOException e) {
			logger.error("DEL Error Opening File " + filePathShortVals);
			return false;
		}

		shortValFileLock.writeLock().lock();
		int res = delFileEntry(raf, key, true);
		shortValFileLock.writeLock().unlock();

		try {
			raf.close();
		} catch (IOException e) {
			logger.error("Error Closing File " + filePathShortVals);
			return false;
		}
		if (res != errSuccess) {
			logger.warn("get key " + key + "failed, res=" + res);
		} else { // deleted from short value file, then do not need to proceed to long value file
			//logger.info("deleted from short value file.");
			return true;
		}

		// Delete key from long value file
		f = new File(filePathLongVals);
		if (!f.isFile()) {
			return true;
		}
		try {
			raf = new RandomAccessFile(filePathLongVals, "rw");
		} catch (IOException e) {
			logger.warn("Error Opening File " + filePathLongVals);
			return false;
		}

		longValFileLock.writeLock().lock();
		res = delFileEntry(raf, key, false);
		longValFileLock.writeLock().unlock();

		try {
			raf.close();
		} catch (IOException e) {
			logger.warn("Error Closing File " + filePathLongVals);
			return false;
		}
		if (res != errSuccess) {
			logger.warn("delete key " + key + "failed, res=" + res);
		}

		return (res == errSuccess);
	}

	/*
	 * Clear file storage
	 */
	public void clear() {
		File file = new File(filePathShortVals); 
		file.delete();

	    file = new File(filePathLongVals); 
		file.delete();
	 }

	/*
	 * Get the key of an entry given index
	 */
	private String getFileEntryKeyByIdx(RandomAccessFile raf, int idx, boolean isShortValFile) {
		int entryLength = shortValEntryLength;
		if (!isShortValFile) {
			entryLength = longValEntryLength;
		}

		try {
			raf.seek(idx * entryLength);
			byte valid = raf.readByte();
			if (valid == (byte) 0) { // invalid
				return null;
			}

			raf.seek(idx * entryLength + validByteLength);
			int keyLen = raf.readInt();
			byte[] valBytes = new byte[keyLen];

			raf.seek(idx * entryLength + validByteLength + keySizeLength);
			raf.read(valBytes);
			String key = new String(valBytes);
			return key;

		} catch (EOFException e) { // entry is empty
			return null;

		} catch (IOException e) {
			logger.error("getFileEntryKeyByIdx IOException" + e.getMessage());
			return null;
		}
	}

	/*
	 * Get the value of an entry given index
	 */
	private String getFileEntryValByIdx(RandomAccessFile raf, int idx, boolean isShortValFile) {
		int entryLength = shortValEntryLength;
		if (!isShortValFile) {
			entryLength = longValEntryLength;
		}
		
		try {
			raf.seek(idx * entryLength);

			byte valid = raf.readByte();
			if (valid == (byte) 0) { // invalid
				return null;
			}

			raf.seek(idx * entryLength + entryValOffset);
			int keyLen = raf.readInt();
			byte[] valBytes = new byte[keyLen];

			raf.seek(idx * entryLength + entryValOffset + valueSizeLength);
			raf.read(valBytes);
			String val = new String(valBytes);
			return val;

		} catch (EOFException e) { // entry is empty
			return null;

		} catch (IOException e) {
			logger.error("getFileEntryValByIdx IOException" + e.getMessage());
			return null;
		}

	}

	/*
	 * Write an entry to file
	 */
	private Integer putFileEntry(RandomAccessFile raf, String key, String val, boolean isShortValFile) {
		int entryLength = shortValEntryLength;
		if (!isShortValFile) {
			entryLength = longValEntryLength;
		}
		int idx = getAvailEntryIdx(raf, key, isShortValFile); // todo: add false case

		try {
			raf.seek(idx * entryLength);
			raf.writeByte(1); // valid
			raf.seek(idx * entryLength + validByteLength);
			raf.writeInt(key.getBytes().length);
			raf.seek(idx * entryLength + validByteLength + keySizeLength);
			raf.write(key.getBytes());
			raf.seek(idx * entryLength + entryValOffset);
			raf.writeInt(val.getBytes().length);
			raf.write(val.getBytes());

			return errSuccess;

		} catch (IOException e) {
			logger.error("putFileEntry IOException" + e.getMessage());
			return errFailed;
		}
	}

	/*
	 * Delete file entry by writing invalid to the first byte
	 */
	private Integer delFileEntry(RandomAccessFile raf, String key, boolean isShortValFile) {
		int entryLength = shortValEntryLength;
		if (!isShortValFile) {
			entryLength = longValEntryLength;
		}

		int entryIdx = getFileEntryIdx(raf, key, isShortValFile);

		if (entryIdx == errEntryNotFound) {
			logger.info("delete key" + key + "failed, entry not found"); 
			return errEntryNotFound;
		}

		// change status to invalid
		try {
			raf.seek(entryIdx * entryLength);
			raf.writeByte(0);
			raf.seek(entryIdx * entryLength);
			byte valid = raf.readByte();

			return errSuccess;

		} catch (IOException e) {
			logger.error("delFileEntry IOException" + e.getMessage());
			return errFailed;
		}
	}

	// Get the index of an entry by key
	private int getFileEntryIdx(RandomAccessFile raf, String key, boolean isShortValFile) {
		int numEntries = numShortValEntries;
		if (!isShortValFile) {
			numEntries = numLongValEntries;
		}

		int idx = getHashVal(key);
		idx = idx % numEntries;
		int numProcessed = 0;
		String idxKey = getFileEntryKeyByIdx(raf, idx, isShortValFile);
		//logger.info("isShortValFile" + isShortValFile + " idxKey " + idxKey + "idx" + idx);

		while (numProcessed < numEntries && idxKey != null && !idxKey.equals(key)) {
			idx++;
			numProcessed++;
			idxKey = getFileEntryKeyByIdx(raf, idx, isShortValFile);
		}

		if (numProcessed == numEntries || idxKey == null) {
			logger.warn("Entry not found, Searched " + numProcessed + " entries"); // todo: change to log
			return errEntryNotFound;
		}

		return idx % numEntries;
	}

	// get an available index
	private int getAvailEntryIdx(RandomAccessFile raf, String key, boolean isShortValFile) {
		int numEntries = numShortValEntries;
		if (!isShortValFile) {
			numEntries = numLongValEntries;
		}

		int idx = getHashVal(key);
		idx = idx % numEntries;
		int numProcessed = 0;

		String idxKey = getFileEntryKeyByIdx(raf, idx, isShortValFile); // read the key of the entry in the current
																		// index
		while (numProcessed < numEntries && idxKey != null && !idxKey.equals(key)) {
			idx++;
			numProcessed++;
			idxKey = getFileEntryKeyByIdx(raf, idx, isShortValFile);
		}
		if (numProcessed == numEntries) {
			String fileName = "ShortFile";
			if (!isShortValFile) {
				fileName = "LongFile";
			}
			logger.error(fileName + "is full");
			return errFailed;
		}

		return idx % numEntries;
	}

	// DJB hash function
	private int getHashVal(String key) {
		int hash = 5381;
		for (int i = 0; i < key.length(); i++) {
			hash = ((hash << 5) + hash) + key.charAt(i);
		}
		if (hash < 0) {
			return -hash;
		}
		return hash;
	}

	public List<String> getAllKeys() {
		int numEntries = numShortValEntries;
		List<String> allKeys = new ArrayList<>();
		String idxKey;

		File f = new File(filePathShortVals);
		RandomAccessFile raf;

		try {
			if (!f.isFile()) {
				logger.error("Command: file does not exist." + filePathShortVals);
				return allKeys;
			}
			raf = new RandomAccessFile(filePathShortVals, "rw");
		} catch (IOException e) {
			logger.error("Error Opening File " + filePathShortVals);
			return allKeys;
		}

		//Search in short value file
		for (int i = 0; i < numEntries; i++){
			idxKey = getFileEntryKeyByIdx(raf, i, true);
			if (idxKey != null) {
				allKeys.add(idxKey);
			}
		}

		try {
			f = new File(filePathLongVals);
			if (!f.isFile()) {
				logger.warn("filePathLongVals does not exist" + filePathLongVals);
				return allKeys;
			}
			raf = new RandomAccessFile(filePathLongVals, "rw");
		} catch (IOException e) {
			logger.error("Error Opening File " + filePathLongVals);
			return allKeys;
		}

		logger.info("Searching long value file");
		
		//Search in long value file
		numEntries = numLongValEntries;
		for (int i = 0; i < numEntries; i++){
			idxKey = getFileEntryKeyByIdx(raf, i, false);
			if (idxKey != null) {
				allKeys.add(idxKey);
			}
		}

		return allKeys;
	}

	// for testing
	// public static void main(String[] args) {
	// System.out.println("Testing FileDB");
	// FileDB fileDB = new FileDB();
	// fileDB.put("k1", "v12345");
	// fileDB.put("k1", "v12346");
	// fileDB.put("k2", "v1239");
	// String v = fileDB.get("k2");
	//
	// System.out.println(v);
	// System.out.println(fileDB.put("k1", null));
	// System.out.println(fileDB.get("k1"));
	//
	// fileDB.put("k1", "v12347");
	// System.out.println(fileDB.get("k1"));
	// }
}
