//! CRC32C abstraction — hardware-accelerated when `std` feature is enabled,
//! software lookup table fallback for `no_std`.

/// Computes CRC32C of the given data.
#[must_use]
#[cfg(feature = "std")]
pub fn crc32c(data: &[u8]) -> u32 {
    ::crc32c::crc32c(data)
}

/// Appends data to an existing CRC32C computation.
#[must_use]
#[cfg(feature = "std")]
pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    ::crc32c::crc32c_append(crc, data)
}

// Software fallback for no_std
#[cfg(not(feature = "std"))]
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

#[cfg(not(feature = "std"))]
static TABLE: [u32; 256] = make_table();

/// Computes CRC32C of the given data (software fallback).
#[must_use]
#[cfg(not(feature = "std"))]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_append(0, data)
}

/// Appends data to an existing CRC32C computation (software fallback).
#[must_use]
#[cfg(not(feature = "std"))]
pub fn crc32c_append(init: u32, data: &[u8]) -> u32 {
    let mut crc = !init;
    for &b in data {
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn append_matches() {
        let full = crc32c(b"hello world");
        let partial = crc32c_append(crc32c_append(0, b"hello "), b"world");
        assert_eq!(full, partial);
    }
}
