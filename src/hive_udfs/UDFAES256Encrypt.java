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
import org.apache.hadoop.io.Text;

public class UDFAES256Encrypt extends GenericUDF{

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		 if (arguments.length < 2) {
			  throw new UDFArgumentException("input must have length 2.");
	     }
		 //Primitive string
		 arguments[0] = (StringObjectInspector) arguments[0];
		 arguments[1] = (StringObjectInspector) arguments[1];
		 
		
		 return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	

	

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		// TODO Auto-generated method stub
		// Get the key, which is the last object of the input
		
		try {
			String toEncrypt = arg0[0].get().toString();
			// Create the key and initializing vector
			byte[][] keySpecs = Utils.createKeyAndIv(Utils.hexStringToByteArray(arg0[1].get().toString()),32);
			// Create SecretKey
			SecretKey sk = new SecretKeySpec(keySpecs[0],"AES");
			// Create iv
			IvParameterSpec iv = new IvParameterSpec(keySpecs[1]);
			// Get Cipher
			Cipher c = Cipher.getInstance("AES/CBC/PKCS5Padding");
			c.init(Cipher.ENCRYPT_MODE, sk, iv);
			// Encrypt the string
			
			return new Text(Utils.toHex(c.doFinal(toEncrypt.getBytes("UTF-8"))).toUpperCase());
					
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}






	@Override
	public String getDisplayString(String[] arg0) {
		return "AES/CBC/PKCS5Padding with 256 bit";
	}
}
