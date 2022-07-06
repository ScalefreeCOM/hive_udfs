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

import hive_udfs.UDFAES256Decrypt;
import hive_udfs.Utils;

class UDFAES256DecryptTest {
	// BD1C60B01C7474BE1AE5433A619FE07B
	@Test
	void test() throws HiveException {
		Text inputText = new Text("69E68E742665B1CE6BC15561AAF8A931");
	    Text inputKey = new Text("secret");
	
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			JavaStringObjectInspector aesInspector = (JavaStringObjectInspector) aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			String aesStringResult = aesInspector.getPrimitiveJavaObject(aesResult);
			System.out.println(aesStringResult);
			assertEquals("scalefree.com",aesStringResult);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	void test_with_Int() throws HiveException{
		Text inputText = new Text("7B758EDF02DEC36646B4E89D135101D0");
	    Text inputKey = new Text("secret");
	
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
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
		Text inputText = new Text("5F6AA3BF1EC8B065571102EF563014C1F9014F06E7B9585FF359E19EAFB8DCFB");
	    Text inputKey = new Text("secret");
	
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
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
		Text inputText = new Text("A032832F8B6E4ED1C9BCA8E2261C9E4B");
	    Text inputKey = new Text("secret");
	
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
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
		Text inputText = new Text("008583EB7AC77717927C20EF82F1DC02462E88E3436F531912FADE0DE466DEBA");
	    Text inputKey = new Text("secret");
	
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
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
		Text inputText = new Text("48DFF8A6692E02629651635C7C508938");
	    Text inputKey = new Text("secret");
		ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		   
		try (UDFAES256Decrypt aes = new UDFAES256Decrypt()) {
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
