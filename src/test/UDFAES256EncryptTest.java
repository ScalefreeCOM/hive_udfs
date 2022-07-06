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

import hive_udfs.UDFAES256Encrypt;
import hive_udfs.UDFKeyGen256;
import hive_udfs.Utils;

class UDFAES256EncryptTest {

	@Test
	void test_with_keyGenerator() throws HiveException, UnsupportedEncodingException {
		try (UDFKeyGen256 r = new UDFKeyGen256()) {
			ObjectInspector keyInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			
			JavaStringObjectInspector resultInspector = (JavaStringObjectInspector) r.initialize(
			        new ObjectInspector[] { keyInspec});
			// ACT
			Object result = r.evaluate(new DeferredObject[] { null });
			String res = resultInspector.getPrimitiveJavaObject(result);
			
			Text inputText = new Text("scalefree.com");
			Text inputKey = new Text(res);
			
			
			try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
				Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(inputKey) });
				
				assertEquals(32,aesResult.toString().getBytes().length);
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
	
	    try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			Object aesResult = (Object )aes.evaluate(new DeferredObject[] {new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
		
	
			assertEquals("69E68E742665B1CE6BC15561AAF8A931",aesResult.toString());
			assertEquals(32,aesResult.toString().getBytes().length);
			
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
	
		try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
		
			assertEquals("7B758EDF02DEC36646B4E89D135101D0",aesResult.toString());
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
	
		try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
			
			assertEquals("5F6AA3BF1EC8B065571102EF563014C1F9014F06E7B9585FF359E19EAFB8DCFB",aesResult.toString());
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
	
		try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });

			assertEquals("A032832F8B6E4ED1C9BCA8E2261C9E4B",aesResult.toString());
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
	
		try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			       
			

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
		
			assertEquals("008583EB7AC77717927C20EF82F1DC02462E88E3436F531912FADE0DE466DEBA",aesResult.toString());
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
	
		try (UDFAES256Encrypt aes = new UDFAES256Encrypt()) {
			ObjectInspector inputInspec = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
			ObjectInspector keyInspec2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

			aes.initialize(
			            new ObjectInspector[] {inputInspec,keyInspec2});
			
			Object aesResult = (Object) aes.evaluate(new DeferredObject[] { new DeferredJavaObject(inputText),new DeferredJavaObject(    Utils.toHex(inputKey.getBytes())) });
	
			assertEquals("48DFF8A6692E02629651635C7C508938",aesResult.toString());
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
}
