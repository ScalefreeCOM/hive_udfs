package hive_udfs;

import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class UDFKeyGen128 extends GenericUDF{
	@Override
	public Text evaluate(DeferredObject[] arg0) throws HiveException {
		try {
			return new Text(Utils.toHex(this.generateKey(128)));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	private byte[] generateKey(int size) throws NoSuchAlgorithmException {
		KeyGenerator kg = KeyGenerator.getInstance("AES");
		kg.init(size);
		return kg.generateKey().getEncoded();
	}
	
	
	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}


}
