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

import hive_udfs.UDFSHA2;

class UDFSHA2Test {


	@Test
	void test() throws HiveException {
		try (// ARANGE
		UDFSHA2 r = new UDFSHA2()) {
			ObjectInspector input = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			Text inputText = new Text("scalefree.com");
			
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			                          new ObjectInspector[] { input});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText)});
			String res = resultInspector.getPrimitiveJavaObject(result);
			// ASSERT
			assertEquals("4479E782C8AA2FAB87A00405D1BA4797184A712C8DEE728306957DCE0C818E5A",res);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
