package hive_udfs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class UDFMD5 extends GenericUDF{
	private ObjectInspector[] args;
	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		if (arg0.length == 0) {
            return null;             
        }
		// Creating the string
		StringBuilder sb = new StringBuilder("UTF-8");
		for(int i = 0; i < arg0.length; i++) {
			sb.append(((StringObjectInspector) this.args[i]).getPrimitiveJavaObject(arg0[i].get()).toString());	
		}
		
		try {
			// Create the md5 hash
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] digest = md.digest(sb.toString().getBytes());
			// Convert Bytes to Hex -> Uppercase -> Wrap with Hive Text Object
			return new Text(Utils.toHex(digest).toUpperCase());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "Generates a MD5 Hash of n arguments by concatening them in the direct order";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		// Check for if any function input
		 if (arg0.length < 1) {
			  throw new UDFArgumentException("input must have length 1 ");
	     }
		 // If ObjectInspector not type of StringObjectInspector cast
		 for(int i = 0; i < arg0.length; i++) {
			 if(!(arg0[i] instanceof StringObjectInspector)) {
				  arg0[i] = (StringObjectInspector) arg0[i];
			  }
		  }
		  // Set reference
		  this.args = arg0;
		  // return a javaStringObjectInspector
		  return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

}
