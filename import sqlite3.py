import mmh3
import struct

def murmurhash_64bit(data):
    # Получаем 32-битный хеш (два значения) и объединяем их в 64-битное значение
    hash_value = mmh3.hash64(data)
    # Возвращаем 8-байтовое представление
    return struct.pack('q', hash_value[0])  # q означает 64-битное число со знаком

# Пример использования
data = "Пример строки"
hash_64bit = murmurhash_64bit(data)
print(f"64-битный хеш в байтовом представлении: {hash_64bit}")
print(f"64-битный хеш в шестнадцатеричном виде: {hash_64bit.hex()}")
