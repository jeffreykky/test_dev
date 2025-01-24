# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook contians the code for Baozun web request data AES Encryption functions
# MAGIC External dependencies
# MAGIC - PIP: pycryptodome

# COMMAND ----------

# MAGIC %md
# MAGIC Sample Python code
# MAGIC ```python
# MAGIC from collections import OrderedDict
# MAGIC import json
# MAGIC
# MAGIC key: str = "<your key goes here>"
# MAGIC
# MAGIC # Create a OrderedDict just to ensure the converted JSON string will always stay the same when comparing the result from other programming languages.
# MAGIC # It is not necessary in a normal run.
# MAGIC request: OrderedDict[str, object] = OrderedDict()
# MAGIC ##### Add your JSON properties here. #####
# MAGIC # request["pageNum"] = 1
# MAGIC # request["pageSize"] = 200
# MAGIC # request["shopId"] = "596771"
# MAGIC # request["startTime"] = ""
# MAGIC # request["endTime"] = ""
# MAGIC
# MAGIC # Default Python object convert to JSON string.
# MAGIC request_string: str = json.dumps(request)
# MAGIC # Or, use the below line to minimize the JSON string output, the ciphertext will be different, but JSON is semantically the same.
# MAGIC # request_string: str = json.dumps(request, separators=(",", ":"))
# MAGIC request_ciphertext: bytes = encrypt_baozun_web_request(key, request_string)
# MAGIC
# MAGIC # Get back the original JSON string.
# MAGIC round_tripped_request_string: str = decrypt_baozun_web_response(key, base64.b64decode(request_ciphertext))
# MAGIC
# MAGIC # Compare the result.
# MAGIC result = request_string == round_tripped_request_string
# MAGIC ```

# COMMAND ----------

def encrypt_baozun_web_request(key: str, plaintext: str) -> bytes:
    import base64
    import hashlib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad


    if key is None or plaintext is None or len(key) == 0 or len(plaintext) == 0:
        return bytes(0)
    
    key_md5_hash: bytes = base64.b64encode(hashlib.md5(key.encode("utf-8")).digest())
    if len(key_md5_hash) == 0:
        raise Exception("Invalid key")

    initialization_vector: bytes = bytes(16)  # All zeros in 128-bit IV, it is actually very insecure!
    cipher: AES = AES.new(key_md5_hash, AES.MODE_CBC, **{
        "iv": initialization_vector
    })
    plaintext_in_bytes: bytes  = plaintext.encode("utf-8")
    padded: bytes = pad(plaintext_in_bytes, cipher.block_size, "pkcs7")
    cipertext: bytes = cipher.encrypt(padded)
    return base64.b64encode(cipertext)

# COMMAND ----------

def sign_baozun_web_request_ciphertext(**kwargs: str) -> str:
    import hashlib


    key: str = kwargs.get("appSecret")
    interface_type: str = kwargs.get("interfaceType")
    method_name: str = kwargs.get("methodName")
    request_time: str = kwargs.get("requestTime")
    source_app: str = kwargs.get("sourceApp")
    version: str = kwargs.get("version")
    ciphertext: str = kwargs.get("ciphertext")

    msg: str = "{0}interfaceType{1}methodName{2}requestTime{3}sourceApp{4}version{5}{6}{0}".format(
        key,
        interface_type,
        method_name,
        request_time,
        source_app,
        version,
        ciphertext
    )

    md5_hash: bytes = hashlib.md5(msg.encode("utf-8")).digest()
    return md5_hash.hex().upper()

# COMMAND ----------

def decrypt_baozun_web_response(key: str, ciphertext_in_bytes: bytes) -> str:
    import base64
    import hashlib
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad


    if key is None or ciphertext_in_bytes is None or len(key) == 0 or len(ciphertext_in_bytes) == 0:
        return bytes(0)
    
    key_md5_hash: bytes = base64.b64encode(hashlib.md5(key.encode("utf-8")).digest())
    if len(key_md5_hash) == 0:
        raise Exception("Invalid key")

    initialization_vector: bytes = bytes(16)  # All zeros in 128-bit IV, it is actually very insecure!
    cipher: AES = AES.new(key_md5_hash, AES.MODE_CBC, **{
        "iv": initialization_vector
    })
    padded: bytes = cipher.decrypt(ciphertext_in_bytes)
    plaintext_in_bytes: bytes = unpad(padded, cipher.block_size, "pkcs7")
    return plaintext_in_bytes.decode("utf-8")
