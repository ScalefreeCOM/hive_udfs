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

import hive_udfs.UDFMD5;


class UDFMD5Test {

	@Test
	void test() throws HiveException {
		try (// ARANGE
		UDFMD5 r = new UDFMD5()) {
			ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			Text inputText = new Text("scalefree.com");
			
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			                          new ObjectInspector[] { input});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText)});
			String res = resultInspector.getPrimitiveJavaObject(result);
			// ASSERT
			assertEquals("CC8EB3A84F0B148CDFDA5A4D55369DA1",res);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	
}
