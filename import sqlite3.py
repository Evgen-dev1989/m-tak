import hashlib


link = 'link'
hash_object = hashlib.sha1(str.encode(link))
hex_dig = hash_object.hexdigest()
 
print(hash_object)