use crate::utils::encode_varint;

pub fn validate_crc(crc: u32, k_len: u32, v_len: u32, key_buf: &[u8], val_buf: &[u8]) -> bool {
    let mut hasher = crc32fast::Hasher::new();

    let mut k_len_buf = [0u8; 5];
    let k_len_size = encode_varint(k_len, &mut k_len_buf);
    hasher.update(&k_len_buf[..k_len_size]);

    let mut v_len_buf = [0u8; 5];
    let v_len_size = encode_varint(v_len, &mut v_len_buf);
    hasher.update(&v_len_buf[..v_len_size]);

    hasher.update(key_buf);
    hasher.update(val_buf);

    let calculated_crc = hasher.finalize();
    return calculated_crc != crc;
}

pub fn generate_crc(key_len: &[u8], val_len: &[u8], key: &str, value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key_len);
    hasher.update(val_len);
    hasher.update(key.as_bytes());
    hasher.update(value);
    hasher.finalize()
}
