//! # RAID5 / RAID6 parity computation
//!
//! Self-contained math for the parity stripes that btrfs's RAID5 and
//! RAID6 profiles require. Two functions:
//!
//! - [`compute_p`]: byte-wise XOR over the data stripes. Used by both
//!   RAID5 and RAID6 (the "P" stripe).
//! - [`compute_p_q`]: XOR plus a Reed-Solomon code over GF(2^8) using
//!   the generator `x^8 + x^4 + x^3 + x^2 + 1` (0x1D). Used by RAID6.
//!
//! Inputs are equal-length byte slices (one per data column of a row).
//! The output buffers are the same length and represent the parity
//! columns of that row.
//!
//! ## GF(2^8) recipe (Q stripe)
//!
//! Q is computed by walking the data stripes in order and accumulating
//! `q = mul2(q) ^ data_byte`, where `mul2` is multiplication by 2 in
//! GF(2^8) with reduction by the polynomial 0x1D when the high bit is
//! set. This matches the kernel's `raid6_call` and produces the same
//! byte sequence the on-disk format expects.
//!
//! The two-times multiplication table is precomputed at first use via
//! [`std::sync::OnceLock`]; the cost is 256 byte writes once per
//! process.

use std::sync::OnceLock;

/// Reduction polynomial of GF(2^8) used by the RAID6 Q stripe:
/// `x^8 + x^4 + x^3 + x^2 + 1`. Only the low 8 bits matter (the
/// implicit `x^8` term is handled by the conditional XOR after the
/// shift).
const RS_POLY: u8 = 0x1D;

/// Lookup table for `x -> 2 * x` in GF(2^8) under [`RS_POLY`].
///
/// Built once at first call; subsequent lookups are O(1).
fn mul2_table() -> &'static [u8; 256] {
    static TABLE: OnceLock<[u8; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [0u8; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            // i is bounded by 256 (array length), so the cast is exact.
            let x = u8::try_from(i).expect("table index < 256");
            // Multiplication by 2 in GF(2^8) is a left shift; if the
            // pre-shift high bit was set (so the result would overflow
            // into x^8) reduce by XORing the polynomial.
            let shifted = x << 1;
            *slot = if x & 0x80 != 0 {
                shifted ^ RS_POLY
            } else {
                shifted
            };
        }
        t
    })
}

/// Multiply `x` by 2 in GF(2^8). Inlined helper around [`mul2_table`].
#[inline]
fn mul2(x: u8) -> u8 {
    mul2_table()[x as usize]
}

/// Compute the RAID5/RAID6 P (XOR) stripe over `data_stripes`.
///
/// Every input slice must have the same length; the returned buffer has
/// that length too. With zero data stripes, returns an empty buffer.
///
/// # Panics
///
/// Panics if the data stripes are not all the same length. Callers
/// inside this crate construct the stripes from a single chunk row, so
/// equal lengths are an invariant — the panic catches programmer
/// errors only.
#[must_use]
pub fn compute_p(data_stripes: &[&[u8]]) -> Vec<u8> {
    let Some(first) = data_stripes.first() else {
        return Vec::new();
    };
    let len = first.len();
    debug_assert!(
        data_stripes.iter().all(|s| s.len() == len),
        "compute_p: stripes must have equal length"
    );
    let mut p = first.to_vec();
    for stripe in &data_stripes[1..] {
        for (out, &b) in p.iter_mut().zip(stripe.iter()) {
            *out ^= b;
        }
    }
    p
}

/// Compute the RAID6 (P, Q) parity stripes over `data_stripes`.
///
/// P is the XOR of all data stripes. Q is the Reed-Solomon code: walk
/// the stripes in order and update `q[i] = mul2(q[i]) ^ data[i]` for
/// every byte position. Returns `(p, q)` with both buffers the same
/// length as each input stripe.
///
/// With zero data stripes, returns two empty buffers.
///
/// # Panics
///
/// Panics if the data stripes are not all the same length. See
/// [`compute_p`] for the rationale.
#[must_use]
pub fn compute_p_q(data_stripes: &[&[u8]]) -> (Vec<u8>, Vec<u8>) {
    let Some(first) = data_stripes.first() else {
        return (Vec::new(), Vec::new());
    };
    let len = first.len();
    debug_assert!(
        data_stripes.iter().all(|s| s.len() == len),
        "compute_p_q: stripes must have equal length"
    );

    let mut p = vec![0u8; len];
    let mut q = vec![0u8; len];

    // Walk stripes in order; each iteration mixes one column into both
    // P (xor) and Q (mul2-then-xor).
    for stripe in data_stripes {
        for i in 0..len {
            p[i] ^= stripe[i];
            q[i] = mul2(q[i]) ^ stripe[i];
        }
    }
    (p, q)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mul2_table_basic_values() {
        // Hand-verifiable entries:
        //   2*0   = 0
        //   2*1   = 2
        //   2*0x40 = 0x80              (high bit not yet set pre-shift)
        //   2*0x80 = (0x100) reduces to 0x1D (poly XOR)
        //   2*0x81 = (0x102) reduces to 0x1F
        //   2*0xFF = (0x1FE) reduces to 0xE3
        let t = mul2_table();
        assert_eq!(t[0x00], 0x00);
        assert_eq!(t[0x01], 0x02);
        assert_eq!(t[0x40], 0x80);
        assert_eq!(t[0x80], 0x1D);
        assert_eq!(t[0x81], 0x1F);
        assert_eq!(t[0xFF], 0xE3);
    }

    #[test]
    fn mul2_doubling_is_associative_to_pow2() {
        // Doubling x four times equals multiplying by 16 in GF(2^8);
        // for x = 1 that should reach 16.
        let mut x = 1u8;
        for _ in 0..4 {
            x = mul2(x);
        }
        assert_eq!(x, 16);
    }

    #[test]
    fn compute_p_empty_input() {
        let p = compute_p(&[]);
        assert!(p.is_empty());
    }

    #[test]
    fn compute_p_single_stripe_is_copy() {
        let s = [0x11u8, 0x22, 0x33, 0x44];
        let p = compute_p(&[&s]);
        assert_eq!(p, s);
    }

    #[test]
    fn compute_p_xor_of_known_pattern() {
        // P of three stripes should be the byte-wise XOR.
        let a = [0xFFu8, 0x00, 0xAA, 0x55];
        let b = [0x0Fu8, 0xF0, 0x55, 0xAA];
        let c = [0x33u8, 0x33, 0x33, 0x33];
        let p = compute_p(&[&a, &b, &c]);
        // Hand-computed:
        //   0xFF^0x0F^0x33 = 0xC3
        //   0x00^0xF0^0x33 = 0xC3
        //   0xAA^0x55^0x33 = 0xCC
        //   0x55^0xAA^0x33 = 0xCC
        assert_eq!(p, vec![0xC3, 0xC3, 0xCC, 0xCC]);
    }

    #[test]
    fn compute_p_xor_of_self_is_zero() {
        // Two copies of the same stripe XOR to zero — sanity check that
        // there's no off-by-one mixing step or extra mul.
        let s = [0xDEu8, 0xAD, 0xBE, 0xEF];
        let p = compute_p(&[&s, &s]);
        assert_eq!(p, vec![0; 4]);
    }

    #[test]
    fn compute_p_q_empty_input() {
        let (p, q) = compute_p_q(&[]);
        assert!(p.is_empty() && q.is_empty());
    }

    #[test]
    fn compute_p_q_zero_data_is_zero() {
        // Two all-zero stripes -> both P and Q must be all-zero.
        let z = [0u8; 8];
        let (p, q) = compute_p_q(&[&z, &z, &z]);
        assert_eq!(p, vec![0; 8]);
        assert_eq!(q, vec![0; 8]);
    }

    #[test]
    fn compute_p_q_single_stripe_p_is_copy_q_equals_stripe() {
        // With one stripe, the loop runs once: p[i] = data[i] (since
        // p starts at 0), q[i] = mul2(0) ^ data[i] = data[i].
        let s = [1u8, 2, 3, 4, 0xFF];
        let (p, q) = compute_p_q(&[&s]);
        assert_eq!(p, s);
        assert_eq!(q, s);
    }

    #[test]
    fn compute_p_q_two_stripes_q_is_2a_xor_b() {
        // For two stripes A and B walked in order:
        //   P[i] = A[i] ^ B[i]
        //   Q[i] = mul2(mul2(0) ^ A[i]) ^ B[i] = mul2(A[i]) ^ B[i]
        let a = [0x01u8, 0x02, 0x40, 0x80];
        let b = [0xFFu8, 0x00, 0xAA, 0x55];
        let (p, q) = compute_p_q(&[&a, &b]);
        let expect_p: Vec<u8> =
            a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect();
        let expect_q: Vec<u8> =
            a.iter().zip(b.iter()).map(|(x, y)| mul2(*x) ^ y).collect();
        assert_eq!(p, expect_p);
        assert_eq!(q, expect_q);
    }

    #[test]
    fn compute_p_q_three_stripes_explicit() {
        // Three stripes; reproduce the loop step by step:
        //   q after stripe 0: q1 = A
        //   q after stripe 1: q2 = mul2(A) ^ B
        //   q after stripe 2: q3 = mul2(mul2(A) ^ B) ^ C
        let s_a = [0x01u8];
        let s_b = [0x40u8];
        let s_c = [0x80u8];
        let (p, q) = compute_p_q(&[&s_a, &s_b, &s_c]);
        assert_eq!(p, vec![0x01 ^ 0x40 ^ 0x80]);
        let expected_q = mul2(mul2(0x01) ^ 0x40) ^ 0x80;
        assert_eq!(q, vec![expected_q]);
    }

    #[test]
    fn compute_p_q_p_consistent_with_compute_p() {
        // For the same data, the P returned by compute_p_q must equal
        // the standalone compute_p output. Sanity check that the two
        // helpers agree.
        let mut a = [0u8; 64];
        let mut b = [0u8; 64];
        let mut c = [0u8; 64];
        for i in 0..64 {
            a[i] = (i as u8).wrapping_mul(7);
            b[i] = (i as u8).wrapping_mul(11);
            c[i] = (i as u8).wrapping_mul(13);
        }
        let p_solo = compute_p(&[&a, &b, &c]);
        let (p_dual, _q) = compute_p_q(&[&a, &b, &c]);
        assert_eq!(p_solo, p_dual);
    }

    #[test]
    fn compute_p_reconstruct_missing_stripe_via_xor() {
        // Algebra check: in RAID5, given P and all-but-one of the data
        // stripes, the missing stripe = XOR(P, other stripes). If our P
        // is correct, this round-trip recovers the input.
        let a = [0x12u8, 0x34, 0x56, 0x78];
        let b = [0x9Au8, 0xBC, 0xDE, 0xF0];
        let c = [0x11u8, 0x22, 0x33, 0x44];
        let p = compute_p(&[&a, &b, &c]);
        // Reconstruct b from P, A, and C.
        let recon: Vec<u8> = p
            .iter()
            .zip(a.iter())
            .zip(c.iter())
            .map(|((p, x), y)| p ^ x ^ y)
            .collect();
        assert_eq!(recon, b);
    }
}
