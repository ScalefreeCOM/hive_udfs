package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import hive_udfs.UDFAES128Encrypt;
import hive_udfs.UDFKeyGen128;
import hive_udfs.Utils;

class UDFAES128EncryptTest {

	@Test
	void test_with_keyGenerator() throws HiveException, UnsupportedEncodingException {
		try (UDFKeyGen128 r = new UDFKeyGen128()) {
			ObjectInspector keyInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			        new ObjectInspector[] { keyInspec});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { null });
			String res = resultInspector.getPrimitiveJavaObject(result);
			
			Text inputText = new Text("scalefree.com");
			
			Text inputKey = new Text(res);
			
			try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
				ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
				ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
				       
				

				JavaStringObjectInspector aesInspector = (JavaStringObjectInspector) aes.initialize(
				            new ObjectInspector[] {inputInspec,keyInspec2});
				
				Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(inputKey) });
				
				String aesStringResult = aesInspector.getPrimitiveJavaObject(aesResult);
				assertEquals(32,aesStringResult.getBytes().length);
			}
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	void test_with_set_keyword() throws HiveException, UnsupportedEncodingException{

	
		
		Text inputText = new Text("scalefree.com");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			JavaStringObjectInspector aesInspector = (JavaStringObjectInspector) aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			String aesStringResult = aesInspector.getPrimitiveJavaObject(aesResult);
			assertEquals("BD1C60B01C7474BE1AE5433A619FE07B",aesStringResult);

			
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	
	
	

	@Test
	void test_with_Int() throws HiveException, UnsupportedEncodingException{
		Text inputText = new Text("1");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			assertEquals("436D62FF896CBA2AAA326C2587DD8212",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	

	@Test
	void test_with_long() throws HiveException, UnsupportedEncodingException{
		Text inputText = new Text("9223372036854775807");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			assertEquals("EBE3291AABDD2F55C0A00AD5C119E275A095FEFFC04A93DD9C99E33602C4D43B",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	
	

	

	@Test
	void test_with_float() throws HiveException, UnsupportedEncodingException{
		Text inputText = new Text("1.23");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
		
			assertEquals("9F2A49BDC771046A0A8CFD5883AAD439",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	

	

	@Test
	void test_with_double() throws HiveException, UnsupportedEncodingException{
		Text inputText = new Text("1.797693134862315");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			assertEquals("5A1CDD14A1E81C9228180470FA189E3414E89AA472876E46898A0F77A1699C39",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	

	

	@Test
	void test_with_boolean() throws HiveException, UnsupportedEncodingException{
		Text inputText = new Text("true");
	    Text inputKey = new Text("secret");
	
		try (UDFAES128Encrypt aes = new UDFAES128Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			assertEquals("4929706D571FC279251CEA75EB40D745",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	
	
}
