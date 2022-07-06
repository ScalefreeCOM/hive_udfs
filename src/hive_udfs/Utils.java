package hive_udfs;

public class Utils {

	
	public static String toHex(byte[] buf) {
	    StringBuffer strbuf = new StringBuffer(buf.length * 2);
        int i;
        for (i = 0; i < buf.length; i++) {
            if (((int) buf[i] & 0xff) < 0x10)
                strbuf.append("0");
            strbuf.append(Long.toString((int) buf[i] & 0xff, 16));
        }
        return strbuf.toString();
	}
	
	public static byte[][] createKeyAndIv(byte[] bs, int blocksize){
		byte[] key = initEmptyByte(blocksize);
		byte[] iv = initEmptyByte(16);

		for(int i =0; i < bs.length; i++) {
			key[i % blocksize] ^= bs[i];
		}
		return new byte[][] {key,iv};
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	private static byte[] initEmptyByte(int size) {
		byte[] returnValue = new byte[size];
		for(int i = 0; i < size; i++) {
			returnValue[i] = 0x00;
		}
		return returnValue;
	}
}
