package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

import hive_udfs.UDFKeyGen128;

class UDFKeyGenTest {

	@Test
	void test() throws HiveException {
		try (// ARANGE
		UDFKeyGen128 r = new UDFKeyGen128()) {
			ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
   
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			        new ObjectInspector[] { input});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { null });
			String res = resultInspector.getPrimitiveJavaObject(result);
			// ASSERT
			assertEquals(32,res.getBytes().length); // 32 byte key
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
