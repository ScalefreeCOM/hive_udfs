package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import hive_udfs.UDFAES128Decrypt;
import hive_udfs.Utils;

class UDFAES128DecryptTest {
	// BD1C60B01C7474BE1AE5433A619FE07B
	@Test
	void test() throws HiveException {
		Text inputText = new Text("BD1C60B01C7474BE1AE5433A619FE07B");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			JavaStringObjectInspector aesInspector = (JavaStringObjectInspector) aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			String aesStringResult = aesInspector.getPrimitiveJavaObject(aesResult);
			assertEquals("scalefree.com",aesStringResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	@Test
	void test_with_Int() throws HiveException{
		Text inputText = new Text("436D62FF896CBA2AAA326C2587DD8212");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			assertEquals(Integer.valueOf(1),aesResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

	@Test
	void test_with_long() throws HiveException{
		Text inputText = new Text("EBE3291AABDD2F55C0A00AD5C119E275A095FEFFC04A93DD9C99E33602C4D43B");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			assertEquals(Long.valueOf(9223372036854775807L),aesResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

	@Test
	void test_with_float() throws HiveException{
		Text inputText = new Text("9F2A49BDC771046A0A8CFD5883AAD439");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			assertEquals(Float.valueOf(1.23f),aesResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	@Test
	void test_with_double() throws HiveException{
		Text inputText = new Text("5A1CDD14A1E81C9228180470FA189E3414E89AA472876E46898A0F77A1699C39");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			assertEquals(Double.valueOf(1.797693134862315),aesResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	@Test
	void test_with_boolean() throws HiveException{
		Text inputText = new Text("4929706D571FC279251CEA75EB40D745");
	    Text inputKey = new Text("secret");
		ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		   
		try (UDFAES128Decrypt aes = new UDFAES128Decrypt()) {
			aes.initialize(
		            new ObjectInspector[] {inputInspec,keyInspec2});
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			assertEquals(true,aesResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
