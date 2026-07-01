//! # file-replicator — bandwidth governor (DESIGN §13.5)
//!
//! A token-bucket rate limiter on the transfer byte-stream. A transfer must pass **both** a
//! per-instance bucket (`limits.maxBandwidth`) and a shared global bucket
//! (`component.global.limits.maxBandwidth`) — [`Bandwidth::throttle`] awaits the larger of the two
//! waits before writing. The bucket takes an injectable [`Clock`] so the wait math is deterministically
//! testable without real sleeps. Human byte-rates like `"20MB/s"`, `"5Mbps"`, `"1Gbps"` are parsed by
//! [`parse_byte_rate`].

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Injectable monotonic time source so bandwidth throttling is deterministically testable.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Production clock backed by [`Instant::now`].
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Test clock advanced by hand via [`advance`](Self::advance).
pub struct ManualClock {
    t: Mutex<Instant>,
}
impl ManualClock {
    pub fn new() -> Self {
        ManualClock {
            t: Mutex::new(Instant::now()),
        }
    }
    /// Advance the clock by `d`.
    pub fn advance(&self, d: Duration) {
        let mut t = self.t.lock().expect("clock mutex");
        *t += d;
    }
}
impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.t.lock().expect("clock mutex")
    }
}

struct BucketState {
    tokens: f64,
    last: Instant,
}

/// A token bucket. `rate` is bytes/sec (`0` = unlimited); burst capacity = one second of rate.
pub struct TokenBucket {
    rate: u64,
    capacity: f64,
    state: Mutex<BucketState>,
    clock: Arc<dyn Clock>,
}

impl TokenBucket {
    /// A bucket refilling at `rate_bytes_per_sec` with a one-second burst capacity. `rate == 0`
    /// yields an unlimited bucket.
    pub fn new(rate_bytes_per_sec: u64, clock: Arc<dyn Clock>) -> Self {
        let capacity = rate_bytes_per_sec as f64;
        let now = clock.now();
        TokenBucket {
            rate: rate_bytes_per_sec,
            capacity,
            // Start full so a fresh transfer can burst immediately up to one second of rate.
            state: Mutex::new(BucketState {
                tokens: capacity,
                last: now,
            }),
            clock,
        }
    }

    /// An unlimited bucket ([`acquire`](Self::acquire) always returns `Duration::ZERO`).
    pub fn unlimited() -> Self {
        TokenBucket::new(0, Arc::new(SystemClock))
    }

    /// Whether this bucket imposes no limit.
    pub fn is_unlimited(&self) -> bool {
        self.rate == 0
    }

    /// Account for `n` bytes and return how long the caller must wait before sending them (`0` if
    /// tokens are already available). Tokens may go negative (debt) so the *average* rate is honored
    /// even for a write larger than the burst capacity.
    pub fn acquire(&self, n: u64) -> Duration {
        if self.rate == 0 {
            return Duration::ZERO;
        }
        let mut s = self.state.lock().expect("bucket mutex");
        let now = self.clock.now();
        let elapsed = now.saturating_duration_since(s.last).as_secs_f64();
        s.tokens = (s.tokens + elapsed * self.rate as f64).min(self.capacity);
        s.last = now;

        let need = n as f64;
        if s.tokens >= need {
            s.tokens -= need;
            Duration::ZERO
        } else {
            let deficit = need - s.tokens;
            s.tokens -= need; // carry the debt forward
            Duration::from_secs_f64(deficit / self.rate as f64)
        }
    }
}

/// The per-instance ∩ global governor passed into
/// [`Destination::deliver`](crate::dest::Destination::deliver). Cheap to clone (two `Arc`s).
#[derive(Clone)]
pub struct Bandwidth {
    per_instance: Arc<TokenBucket>,
    global: Arc<TokenBucket>,
}

impl Bandwidth {
    /// Combine a per-instance and a global bucket.
    pub fn new(per_instance: Arc<TokenBucket>, global: Arc<TokenBucket>) -> Self {
        Bandwidth {
            per_instance,
            global,
        }
    }

    /// Both buckets unlimited.
    pub fn unlimited() -> Self {
        Bandwidth {
            per_instance: Arc::new(TokenBucket::unlimited()),
            global: Arc::new(TokenBucket::unlimited()),
        }
    }

    /// Reserve `bytes` from both buckets and await the larger of the two required waits before the
    /// caller writes.
    pub async fn throttle(&self, bytes: u64) {
        let wait = self
            .per_instance
            .acquire(bytes)
            .max(self.global.acquire(bytes));
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Parse a human byte-rate into **bytes per second**.
///
/// Accepts SI-decimal byte units (`KB`, `MB`, `GB`, `TB` = 1000ⁿ), binary byte units
/// (`KiB`, `MiB`, `GiB`, `TiB` = 1024ⁿ), a bare byte size (per second), and bit-rate units
/// (`bps`, `Kbps`, `Mbps`, `Gbps` — decimal bits, divided by 8). An optional trailing `/s` or `/sec`
/// on byte units is allowed and ignored. Whitespace and case are insignificant.
///
/// Examples: `"20MB/s"` → 20_000_000, `"1Gbps"` → 125_000_000, `"512"` → 512, `"1MiB"` → 1_048_576.
pub fn parse_byte_rate(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty byte-rate".to_string());
    }
    let lower = s.to_ascii_lowercase();

    // Bit rates end in "bps"; byte rates may carry a trailing "/s" or "/sec" (ignored).
    let (body, is_bits) = if let Some(stripped) = lower.strip_suffix("bps") {
        (stripped, true)
    } else if let Some(stripped) = lower.strip_suffix("/sec") {
        (stripped, false)
    } else if let Some(stripped) = lower.strip_suffix("/s") {
        (stripped, false)
    } else {
        (lower.as_str(), false)
    };
    let body = body.trim();

    // Split leading number (digits + optional '.') from the trailing unit.
    let split = body
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(body.len());
    let (num_str, unit_raw) = body.split_at(split);
    let num_str = num_str.trim();
    if num_str.is_empty() {
        return Err(format!("no numeric value in byte-rate {input:?}"));
    }
    let value: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid number in byte-rate {input:?}"))?;
    if value < 0.0 || !value.is_finite() {
        return Err(format!("byte-rate must be non-negative and finite: {input:?}"));
    }

    let mut unit = unit_raw.trim();
    // For byte units, drop the trailing 'b'/'byte(s)'. Bit units already had "bps" stripped.
    if !is_bits {
        unit = unit
            .strip_suffix("bytes")
            .or_else(|| unit.strip_suffix("byte"))
            .or_else(|| unit.strip_suffix('b'))
            .unwrap_or(unit);
    }

    let prefix_mult = match unit {
        "" => 1.0,
        "k" => 1e3,
        "ki" => 1024.0,
        "m" => 1e6,
        "mi" => 1024f64.powi(2),
        "g" => 1e9,
        "gi" => 1024f64.powi(3),
        "t" => 1e12,
        "ti" => 1024f64.powi(4),
        other => return Err(format!("unknown byte-rate unit {other:?} in {input:?}")),
    };

    let mut bytes_per_sec = value * prefix_mult;
    if is_bits {
        bytes_per_sec /= 8.0;
    }
    Ok(bytes_per_sec.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manual() -> Arc<ManualClock> {
        Arc::new(ManualClock::new())
    }

    #[test]
    fn parse_byte_units_decimal() {
        assert_eq!(parse_byte_rate("20MB/s").unwrap(), 20_000_000);
        assert_eq!(parse_byte_rate("20 mb/s").unwrap(), 20_000_000);
        assert_eq!(parse_byte_rate("1KB").unwrap(), 1_000);
        assert_eq!(parse_byte_rate("2GB/sec").unwrap(), 2_000_000_000);
        assert_eq!(parse_byte_rate("512").unwrap(), 512);
        assert_eq!(parse_byte_rate("512B/s").unwrap(), 512);
    }

    #[test]
    fn parse_byte_units_binary() {
        assert_eq!(parse_byte_rate("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_rate("1MiB/s").unwrap(), 1_048_576);
        assert_eq!(parse_byte_rate("1GiB").unwrap(), 1_073_741_824);
    }

    #[test]
    fn parse_bit_rates() {
        assert_eq!(parse_byte_rate("1Gbps").unwrap(), 125_000_000);
        assert_eq!(parse_byte_rate("8bps").unwrap(), 1);
        assert_eq!(parse_byte_rate("100Mbps").unwrap(), 12_500_000);
        assert_eq!(parse_byte_rate("5mbps").unwrap(), 625_000);
    }

    #[test]
    fn parse_decimal_value() {
        assert_eq!(parse_byte_rate("1.5MB/s").unwrap(), 1_500_000);
    }

    #[test]
    fn parse_errors() {
        assert!(parse_byte_rate("").is_err());
        assert!(parse_byte_rate("   ").is_err());
        assert!(parse_byte_rate("MB/s").is_err()); // no number
        assert!(parse_byte_rate("20XB/s").is_err()); // unknown unit
        assert!(parse_byte_rate("-5MB/s").is_err()); // negative
    }

    #[test]
    fn unlimited_never_waits() {
        let b = TokenBucket::unlimited();
        assert!(b.is_unlimited());
        assert_eq!(b.acquire(1_000_000), Duration::ZERO);
    }

    #[test]
    fn acquire_within_capacity_is_free() {
        let clock = manual();
        // 1000 B/s → capacity 1000, starts full.
        let b = TokenBucket::new(1000, clock.clone());
        assert_eq!(b.acquire(1000), Duration::ZERO); // drains the full burst
        // Next 1000 with no refill → full-second deficit.
        assert_eq!(b.acquire(1000), Duration::from_secs_f64(1.0));
    }

    #[test]
    fn acquire_partial_deficit() {
        let clock = manual();
        let b = TokenBucket::new(1000, clock.clone());
        assert_eq!(b.acquire(1000), Duration::ZERO); // empty now
        // Ask for 500 with 0 tokens → wait 0.5s.
        assert_eq!(b.acquire(500), Duration::from_secs_f64(0.5));
    }

    #[test]
    fn acquire_refills_after_advance() {
        let clock = manual();
        let b = TokenBucket::new(1000, clock.clone());
        assert_eq!(b.acquire(1000), Duration::ZERO); // drain
        clock.advance(Duration::from_millis(500)); // +500 tokens
        assert_eq!(b.acquire(500), Duration::ZERO);
        // Now empty again.
        assert_eq!(b.acquire(1000), Duration::from_secs_f64(1.0));
    }

    #[test]
    fn capacity_caps_burst_accumulation() {
        let clock = manual();
        let b = TokenBucket::new(1000, clock.clone());
        assert_eq!(b.acquire(1000), Duration::ZERO); // drain to 0
        clock.advance(Duration::from_secs(10)); // would refill 10_000, capped at 1000
        assert_eq!(b.acquire(1000), Duration::ZERO); // exactly one burst available
        assert_eq!(b.acquire(1), Duration::from_secs_f64(1.0 / 1000.0));
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_takes_max_of_two_waits() {
        // The two-bucket invariant (FR-REL-6): a transfer waits the SLOWER of the per-instance and
        // global buckets. Under paused tokio time the awaited sleep auto-advances the clock by exactly
        // the wait, so `tokio::time::Instant::elapsed` measures the wait the buckets computed.
        let clock = manual();
        // per-instance fast (2000 B/s), global slow (1000 B/s) → the global bucket must govern.
        let per = Arc::new(TokenBucket::new(2000, clock.clone()));
        let global = Arc::new(TokenBucket::new(1000, clock.clone()));
        let bw = Bandwidth::new(per, global);
        // First 1000 is within both initial bursts → no wait.
        let start = tokio::time::Instant::now();
        bw.throttle(1000).await;
        assert_eq!(start.elapsed(), Duration::ZERO, "first burst is free");
        // Now per has 1000 tokens left, global has 0. The next 1000: per deficit 0 (still 0s), global
        // deficit 1000 → 1s. The throttle must wait the global (slower) 1s, not the per-instance 0s.
        let start = tokio::time::Instant::now();
        bw.throttle(1000).await;
        assert_eq!(
            start.elapsed(),
            Duration::from_secs(1),
            "global bucket governs: waited its 1s deficit, not the per-instance 0s"
        );
    }

    #[test]
    fn global_bucket_is_shared_across_instances() {
        // FR-REL-6 requires a GLOBAL aggregate cap shared across instances, not just per-instance.
        // Two instances' Bandwidth governors share one global bucket: once instance A drains the
        // shared burst, instance B must wait — proving the cap is genuinely shared.
        let clock = manual();
        let global = Arc::new(TokenBucket::new(1000, clock.clone()));
        let a = Bandwidth::new(Arc::new(TokenBucket::new(0, clock.clone())), global.clone());
        let b = Bandwidth::new(Arc::new(TokenBucket::new(0, clock.clone())), global.clone());
        // A drains the shared 1000-byte burst (its per-instance bucket is unlimited).
        assert_eq!(a.per_instance.acquire(1000), Duration::ZERO);
        assert_eq!(global.acquire(1000), Duration::ZERO);
        // B now sees the shared global bucket empty → its next byte must wait, even though B's own
        // per-instance bucket is unlimited. This is the cross-instance aggregate cap.
        assert_eq!(b.per_instance.acquire(1000), Duration::ZERO, "B per-instance unlimited");
        assert!(global.acquire(1000) > Duration::ZERO, "shared global cap makes B wait");
    }

    #[tokio::test]
    async fn unlimited_throttle_is_immediate() {
        Bandwidth::unlimited().throttle(10_000_000).await;
    }
}
