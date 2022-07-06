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

public class UDFSHA1 extends GenericUDF{
	private ObjectInspector[] args;
	
	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		if (arg0.length == 0) {
            return new Text("Null");             
        }
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < arg0.length; i++) {
			sb.append(((StringObjectInspector) this.args[i]).getPrimitiveJavaObject(arg0[i].get()).toString());	
		}
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			byte[] digest = md.digest(sb.toString().getBytes());
			return new Text(asHex(digest).toUpperCase());
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		 if (arg0.length < 1) {
			  throw new UDFArgumentException("input must have length 1 ");
	     }
		  
		 for(int i = 0; i < arg0.length; i++) {
			 if(!(arg0[i] instanceof StringObjectInspector)) {
				  arg0[i] = (StringObjectInspector) arg0[i];
			  }
		  }
		  this.args = arg0;
		  return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}


	public String asHex(byte buf[]) {
        StringBuffer strbuf = new StringBuffer(buf.length * 2);
        int i;
        for (i = 0; i < buf.length; i++) {
            if (((int) buf[i] & 0xff) < 0x10)
                strbuf.append("0");
            strbuf.append(Long.toString((int) buf[i] & 0xff, 16));
        }
        return strbuf.toString();
    }
}
