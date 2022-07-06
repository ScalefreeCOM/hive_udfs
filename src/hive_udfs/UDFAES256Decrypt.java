package hive_udfs;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


public class UDFAES256Decrypt extends GenericUDF{
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		 if (arguments.length != 2) {
			  throw new UDFArgumentException("input must have length 1 at least.");
	     }
		 //Primitive string
		 for(int i = 0; i < arguments.length; i++) {
			 if(!(arguments[i] instanceof StringObjectInspector)) {
				 throw new UDFArgumentException("arguments must be of type string");
			 }
		 }
		 return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		// Get the key, which is the last object of the input
	
		try {
			String toDecrypt = arg0[0].get().toString();
			String key = arg0[1].get().toString();
			// Create the key and initializing vector using 16 bytes
			byte[][] keySpecs = Utils.createKeyAndIv(Utils.hexStringToByteArray(key),32);
			// Create SecretKey
			SecretKey sk = new SecretKeySpec(keySpecs[0],"AES");
			// Create iv
			IvParameterSpec iv = new IvParameterSpec(keySpecs[1]);
			// Get Cipher
			Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			c.init(Cipher.DECRYPT_MODE, sk, iv);
			// Unhex -> decrypt the string
			byte[] res = c.doFinal(Utils.hexStringToByteArray(toDecrypt));
			
			// wrap with Text for hive
			return cast(new String(res));
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private <T> Object cast(String o) {
		// Calls in this row: int, long, float, double, string, boolean
		Object parsedObject = tryCastInteger(o);
		if(parsedObject != null) {
			return (int)parsedObject;
		}
		
		parsedObject = tryCastLong(o);
		if(parsedObject != null) {
			return (long)parsedObject;
		}
		parsedObject = tryCastFloat(o);
		if(parsedObject != null) {
			return (float)parsedObject;
		}
		parsedObject = tryCastDouble(o);
		if(parsedObject != null) {
			return (double)parsedObject;
		}
		
		parsedObject = tryCastBoolean(o);
		if(parsedObject != null) {
			return (boolean) parsedObject;
		}
		//Is String
		return o;
	}
	
	private <T> Object tryCastInteger(String o) {
		try {
		 int c = Integer.parseInt(o);
		 if(o.equals(String.valueOf(c))) return c;
		 return null;
		}catch(NumberFormatException e) {}
		return null;
	}
	
	private <T> Object tryCastLong(String o) {
		try {
		 long c = Long.parseLong(o);
		 if(o.equals(String.valueOf(c))) return c;
		 return null;
		}catch(NumberFormatException e) {}
		return null;
	}
	

	private <T> Object tryCastFloat(String o) {
		try {
		 float c = Float.parseFloat(o);
		 if(o.equals(String.valueOf(c))) return c;
		 return null;
		}
		catch(NumberFormatException e){}
		return null;
	}
	
	private <T> Object tryCastDouble(String o) {
		try {
		 double c = Double.parseDouble(o);
		 if(o.equals(String.valueOf(c))) return c;
		 return null;
		}
		catch(NumberFormatException e){}
		return null;
	}
	
	private <T> Object tryCastBoolean(String o) {
		if(o.toLowerCase().equals("true")) return true;
		if(o.toLowerCase().equals("false")) return false;
		
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "Returns the decrypted value of a hex string. Encoded with AES256/CBC/PKCS5Padding.";
	}
}
