package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import hive_udfs.UDFSHA1;

class UDFSHA1Test {

	@Test
	void test() throws HiveException {
		try (// ARANGE
		UDFSHA1 r = new UDFSHA1()) {
			ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			Text inputText = new Text("scalefree.com");
			
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			                          new ObjectInspector[] { input});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText)});
			String res = resultInspector.getPrimitiveJavaObject(result);
			// ASSERT
			assertEquals("251997C9B2D3CE133CE5EF2035346ADBCFCB353E",res);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
