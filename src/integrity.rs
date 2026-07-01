//! # file-replicator — streaming integrity (DESIGN §13.1)
//!
//! One-pass streaming checksums computed *while* bytes are read/written, so integrity never costs a
//! second read of the file (DESIGN §11.4/§13.1). CRC32C is the default (cheap, hardware-accelerated);
//! SHA-256 is available for stronger guarantees. [`verify`] applies the configured
//! [`Verify`](crate::config::Verify) policy — re-hash comparison, size comparison, or none — mapping
//! a mismatch to [`ReplError::Integrity`] so the retry engine counts it separately.

use std::io::{self, Read};

use sha2::{Digest, Sha256};

use crate::config::Verify;
use crate::domain::Checksum;
use crate::error::{ReplError, Result};

/// A checksum algorithm selectable per destination (`egress.checksumAlgorithm`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// CRC32C (Castagnoli) — the default; hardware-accelerated, adequate for transfer integrity.
    Crc32c,
    /// SHA-256 — stronger, for content-addressing / tamper-evidence needs.
    Sha256,
}

impl Algorithm {
    /// Parse a config algorithm name (case-insensitive): `"CRC32C"` | `"SHA256"` / `"SHA-256"`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().replace('-', "").as_str() {
            "CRC32C" => Some(Algorithm::Crc32c),
            "SHA256" => Some(Algorithm::Sha256),
            _ => None,
        }
    }
}

/// An incremental, single-pass hasher. Feed bytes with [`update`](Self::update) as they stream, then
/// call [`finish`](Self::finish) once for the final [`Checksum`].
pub enum Hasher {
    Crc32c(u32),
    Sha256(Box<Sha256>),
}

impl Hasher {
    /// Start a hasher for `algo` (CRC32C seed 0 / fresh SHA-256 state).
    pub fn new(algo: Algorithm) -> Self {
        match algo {
            Algorithm::Crc32c => Hasher::Crc32c(0),
            Algorithm::Sha256 => Hasher::Sha256(Box::new(Sha256::new())),
        }
    }

    /// Fold `buf` into the running digest.
    pub fn update(&mut self, buf: &[u8]) {
        match self {
            Hasher::Crc32c(state) => *state = crc32c::crc32c_append(*state, buf),
            Hasher::Sha256(h) => h.update(buf),
        }
    }

    /// Consume the hasher and return the final [`Checksum`].
    pub fn finish(self) -> Checksum {
        match self {
            Hasher::Crc32c(state) => Checksum::Crc32c(state),
            Hasher::Sha256(h) => {
                let out = h.finalize();
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(&out);
                Checksum::Sha256(bytes)
            }
        }
    }
}

/// Stream `reader` to end, returning `(bytes_read, checksum)` in a single pass. The 64 KiB buffer
/// keeps memory flat regardless of file size.
pub fn hash_reader<R: Read>(reader: &mut R, algo: Algorithm) -> io::Result<(u64, Checksum)> {
    let mut hasher = Hasher::new(algo);
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hasher.finish()))
}

/// Compare two checksums, returning [`ReplError::Integrity`] on mismatch. `Checksum::None` on either
/// side is treated as "not comparable" and accepted (the caller chose not to hash).
pub fn verify_checksum(expected: &Checksum, actual: &Checksum) -> Result<()> {
    if matches!(expected, Checksum::None) || matches!(actual, Checksum::None) {
        return Ok(());
    }
    if expected == actual {
        Ok(())
    } else {
        Err(ReplError::Integrity(format!(
            "checksum mismatch: expected {expected:?}, got {actual:?}"
        )))
    }
}

/// Compare byte counts, returning [`ReplError::Integrity`] on mismatch.
pub fn verify_size(expected: u64, actual: u64) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(ReplError::Integrity(format!(
            "size mismatch: expected {expected}, got {actual}"
        )))
    }
}

/// Apply the configured [`Verify`] policy to a delivered object (DESIGN §13.1):
///
/// - [`Verify::Checksum`] → re-hash comparison ([`verify_checksum`]);
/// - [`Verify::Size`] → byte-count comparison ([`verify_size`]);
/// - [`Verify::None`] → always Ok.
pub fn verify(
    policy: Verify,
    expected_size: u64,
    actual_size: u64,
    expected_checksum: &Checksum,
    actual_checksum: &Checksum,
) -> Result<()> {
    match policy {
        Verify::Checksum => verify_checksum(expected_checksum, actual_checksum),
        Verify::Size => verify_size(expected_size, actual_size),
        Verify::None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known vectors for the ASCII bytes of "123456789".
    const CHECK: &[u8] = b"123456789";
    const CRC32C_CHECK: u32 = 0xE3069283; // Castagnoli check value.

    #[test]
    fn algorithm_from_name() {
        assert_eq!(Algorithm::from_name("crc32c"), Some(Algorithm::Crc32c));
        assert_eq!(Algorithm::from_name("CRC32C"), Some(Algorithm::Crc32c));
        assert_eq!(Algorithm::from_name("sha256"), Some(Algorithm::Sha256));
        assert_eq!(Algorithm::from_name("SHA-256"), Some(Algorithm::Sha256));
        assert_eq!(Algorithm::from_name("md5"), None);
    }

    #[test]
    fn crc32c_known_vector() {
        let mut r = CHECK;
        let (n, ck) = hash_reader(&mut r, Algorithm::Crc32c).unwrap();
        assert_eq!(n, 9);
        assert_eq!(ck, Checksum::Crc32c(CRC32C_CHECK));
    }

    #[test]
    fn sha256_known_vector() {
        // SHA-256("abc")
        let expected = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        let mut r: &[u8] = b"abc";
        let (n, ck) = hash_reader(&mut r, Algorithm::Sha256).unwrap();
        assert_eq!(n, 3);
        assert_eq!(ck, Checksum::Sha256(expected));
    }

    #[test]
    fn streaming_equals_oneshot() {
        // Feeding in chunks must equal hashing the whole buffer at once.
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();

        let mut one = data.as_slice();
        let (_, oneshot) = hash_reader(&mut one, Algorithm::Crc32c).unwrap();

        let mut h = Hasher::new(Algorithm::Crc32c);
        for chunk in data.chunks(7) {
            h.update(chunk);
        }
        assert_eq!(h.finish(), oneshot);

        // Same for SHA-256.
        let mut one = data.as_slice();
        let (_, oneshot) = hash_reader(&mut one, Algorithm::Sha256).unwrap();
        let mut h = Hasher::new(Algorithm::Sha256);
        for chunk in data.chunks(9999) {
            h.update(chunk);
        }
        assert_eq!(h.finish(), oneshot);
    }

    #[test]
    fn empty_input() {
        let mut r: &[u8] = b"";
        let (n, ck) = hash_reader(&mut r, Algorithm::Crc32c).unwrap();
        assert_eq!(n, 0);
        assert_eq!(ck, Checksum::Crc32c(0));
    }

    #[test]
    fn verify_checksum_ok_and_mismatch() {
        assert!(verify_checksum(&Checksum::Crc32c(1), &Checksum::Crc32c(1)).is_ok());
        let e = verify_checksum(&Checksum::Crc32c(1), &Checksum::Crc32c(2)).unwrap_err();
        assert!(matches!(e, ReplError::Integrity(_)));
    }

    #[test]
    fn verify_checksum_none_is_accepted() {
        assert!(verify_checksum(&Checksum::None, &Checksum::Crc32c(1)).is_ok());
        assert!(verify_checksum(&Checksum::Crc32c(1), &Checksum::None).is_ok());
    }

    #[test]
    fn verify_size_ok_and_mismatch() {
        assert!(verify_size(10, 10).is_ok());
        assert!(matches!(
            verify_size(10, 11).unwrap_err(),
            ReplError::Integrity(_)
        ));
    }

    #[test]
    fn verify_policy_dispatch() {
        // checksum policy: compares checksums, ignores size.
        assert!(verify(
            Verify::Checksum,
            1,
            999,
            &Checksum::Crc32c(5),
            &Checksum::Crc32c(5)
        )
        .is_ok());
        assert!(verify(
            Verify::Checksum,
            1,
            1,
            &Checksum::Crc32c(5),
            &Checksum::Crc32c(6)
        )
        .is_err());

        // size policy: compares sizes, ignores checksum.
        assert!(verify(Verify::Size, 7, 7, &Checksum::Crc32c(5), &Checksum::Crc32c(6)).is_ok());
        assert!(verify(Verify::Size, 7, 8, &Checksum::Crc32c(5), &Checksum::Crc32c(5)).is_err());

        // none policy: always ok.
        assert!(verify(Verify::None, 1, 2, &Checksum::Crc32c(5), &Checksum::Crc32c(6)).is_ok());
    }
}
