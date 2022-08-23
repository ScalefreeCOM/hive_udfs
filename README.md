
# hive_udfs

This udf package can be used with our impala udfs, which can be found  [here].

## Methods

All methods are in the package: hive_udfs

### Hash
    UDFMD5  (arg:String) -> String (Hex)
    UDFSHA1 (arg:String) -> String (Hex)
    UDFSHA2 (arg:String) -> String (Hex)


### Key Generation
    UDFKeyGen128 (arg:id) -> String (Hex)
    UDFKeyGen256 (arg:id) -> String (Hex)

Pass an id, to ensure different results for each call. Otherwise hive is not able to reset the salt.

### Encryption/Decryption using AES/CBC/PKCS5Padding
arg: Text to encrypt/decrypt, arg1: password or key in hex

    
    UDFAES128Encrypt (arg:String, arg1:String (Hex) ) -> String (Hex)
    UDFAES128Decrypt (arg:String, arg1:String (Hex) ) -> primitive type
    UDFAES256Encrypt (arg:String, arg1:String (Hex) ) -> String (Hex)
    UDFAES256Decrypt (arg:String, arg1:String (Hex) ) -> primitive type

## Development
Download the Java JCA Jar and add it via external Jar to the project.
Download the necessary Hadoop/Hive Jars and add it via external JAR to your project.


### Upload the UDFs
1) Create a JAR from the UDFS package.
2) Upload the JAR f.e. via SCP to the cluster
```sh
scp -r -P2222 <path>/<udf.jar> <user>@<address>:/<path>
```
3) Register the JAR to the class path via hive shell
```sh
ADD JAR <path>/<udf.jar>;
```
4) The registered JAR should be visible via:
```sh
LIST JARS;
```
5) Create a function from the regisitered JAR.
```sh
CREATE TEMPORARY FUNCTION <alias> AS '<package>.<method>';
```
6) After you can test your functions as usual:
```sh
SELECT encaes128("scalefree.com",hex("secret"));
```






   [here]: <https://github.com/ScalefreeCOM/impala-crypto-udf>
  
