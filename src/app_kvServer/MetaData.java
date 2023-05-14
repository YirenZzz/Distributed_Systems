package app_kvServer;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MetaData {

    private final List<String> hashRing;  // List of all hash values in the ring
    private final Map<String, Range> rangeMap;  // Map of server IP:port to range

    public MetaData(List<String> hashRing, Map<String, Range> rangeMap) {
        this.hashRing = hashRing;
        this.rangeMap = rangeMap;
    }

    public List<String> getHashRing() {
        return hashRing;
    }

    public Map<String, Range> getRangeMap() {
        return rangeMap;
    }

    public boolean containsServer(String server) {
        return rangeMap.containsKey(server);
    }

    public Range getRange(String server) {
        return rangeMap.get(server);
    }

    private String getMD5Hash(String data) throws NoSuchAlgorithmException {
        // Get an instance of MessageDigest with the MD5 algorithm
		MessageDigest md = MessageDigest.getInstance("MD5");
       	// Convert the input string to a byte array and compute the hash
	    byte[] hashBytes = md.digest(data.getBytes());
        
		// Convert the hash to a hex string representation
		StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    
    public String getServer(String key) throws Exception{
        try{
            String hash = getMD5Hash(key);
            for (String server : hashRing) {
            if (rangeMap.get(server).contains(hash)) {
                return server;
            }
        }
        } catch (Exception e) {
			throw e;
		}
        
        
        return null;
    }

    public void addServer(String server, Range range) {
        rangeMap.put(server, range);
    }

    public void removeServer(String server) {
        rangeMap.remove(server);
    }

    public void print() {
        System.out.println("Metadata:");
        System.out.println("Hash ring: " + hashRing);
        for (Map.Entry<String, Range> entry : rangeMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().toString());
        }
    }

    public static class Range {
        private final String start;
        private final String end;

        public Range(String start, String end) {
            this.start = start;
            this.end = end;
        }

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }

        public boolean contains(String hash) {
            if (start.compareTo(end) < 0) {
                // Range does not wrap around
                return hash.compareTo(start) >= 0 && hash.compareTo(end) < 0;
            } else {
                // Range wraps around
                return hash.compareTo(start) >= 0 || hash.compareTo(end) < 0;
            }
        }

        @Override
        public String toString() {
            return "[" + start + ", " + end + ")";
        }
    }
}
