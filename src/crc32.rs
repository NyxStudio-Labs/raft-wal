//! CRC32C (Castagnoli) with a 256-entry lookup table.
//!
//! Polynomial: 0x1EDC6F41 (reflected: 0x82F63B78).
//! Used by LevelDB, RocksDB, and most modern storage systems.

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u32; 256] = make_table();

pub fn crc32c(data: &[u8]) -> u32 {
    finalize(update(INIT, data))
}

pub const INIT: u32 = !0u32;

pub fn update(crc: u32, data: &[u8]) -> u32 {
    let mut crc = crc;
    for &b in data {
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

pub fn finalize(crc: u32) -> u32 {
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // CRC32C of empty is 0x00000000
        assert_eq!(crc32c(b""), 0x0000_0000);
        // CRC32C("123456789") = 0xE3069283
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn deterministic() {
        let data = b"hello raft-wal";
        assert_eq!(crc32c(data), crc32c(data));
    }
}
