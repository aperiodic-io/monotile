//! `pow` as the Proton fork computes it: musl's (Arm optimized-routines, 2018) non-FMA path,
//! ported line by line from base/glibc-compatibility/musl/pow.c. The host libm (glibc, FMA
//! variant) differs from it by 1 ULP in about 1 of 1,000 inputs, which changes the bytes of
//! `skew_samp` (`m3 / pow(var, 1.5)`) and `kurt_samp` (`m4 / pow(var, 2)`).
//!
//! Only what a variance reaches is ported: a positive normal `x` and a moderate `y` with a
//! result between e^-512 and e^512. Anything else uses the host libm.
use crate::pow_tables::*;

fn d(b: u64) -> f64 {
    f64::from_bits(b)
}

fn top12(x: f64) -> u32 {
    (x.to_bits() >> 52) as u32
}

/// log(x) as hi + lo, with about 15 extra bits in lo.
fn log_inline(ix: u64) -> (f64, f64) {
    const OFF: u64 = 0x3fe6955500000000;
    let tmp = ix.wrapping_sub(OFF);
    let i = 3 * ((tmp >> (52 - 7)) % 128) as usize;
    let kd = ((tmp as i64) >> 52) as f64;
    let iz = ix.wrapping_sub(tmp & (0xfff << 52));
    let (z, invc, logc, logctail) = (d(iz), d(LOG_TAB[i]), d(LOG_TAB[i + 1]), d(LOG_TAB[i + 2]));
    // split z so that rhi, rlo and rhi * rhi are exact
    let zhi = d(iz.wrapping_add(1 << 31) & (u64::MAX << 32));
    let (rhi, rlo) = (zhi * invc - 1.0, (z - zhi) * invc);
    let r = rhi + rlo;
    let t1 = kd * d(LN2HI) + logc;
    let t2 = t1 + r;
    let lo1 = kd * d(LN2LO) + logctail;
    let lo2 = t1 - t2 + r;
    let a = POLY.map(d);
    let (ar, arhi) = (a[0] * r, a[0] * rhi);
    let (ar2, arhi2) = (r * ar, rhi * arhi);
    let ar3 = r * ar2;
    let hi = t2 + arhi2;
    let lo3 = rlo * (ar + arhi);
    let lo4 = t2 - hi + arhi2;
    let p = ar3 * (a[1] + r * a[2] + ar2 * (a[3] + r * a[4] + ar2 * (a[5] + r * a[6])));
    let lo = lo1 + lo2 + lo3 + lo4 + p;
    let y = hi + lo;
    (y, hi - y + lo)
}

/// exp(x + xtail); `None` when |x| >= 512 (musl's overflow/underflow special case).
fn exp_inline(x: f64, xtail: f64) -> Option<f64> {
    const TINY: u32 = 0x3c9; // top12(0x1p-54)
    let abstop = top12(x) & 0x7ff;
    if abstop.wrapping_sub(TINY) >= 0x408 - TINY {
        return (abstop < TINY).then_some(1.0 + x);
    }
    let kd = d(INVLN2N) * x + d(SHIFT);
    let ki = kd.to_bits();
    let kd = kd - d(SHIFT);
    let r = x + kd * d(NEGLN2HIN) + kd * d(NEGLN2LON) + xtail;
    let idx = 2 * (ki % 128) as usize;
    let (tail, scale) = (d(EXP_TAB[idx]), d(EXP_TAB[idx + 1].wrapping_add(ki << (52 - 7))));
    let c = EXP_POLY.map(d);
    let r2 = r * r;
    let tmp = tail + r + r2 * (c[0] + r * c[1]) + r2 * r2 * (c[2] + r * c[3]);
    Some(scale + scale * tmp)
}

pub fn pow(x: f64, y: f64) -> f64 {
    let (topx, topy) = (top12(x), top12(y) & 0x7ff);
    if topx.wrapping_sub(1) >= 0x7fe || topy.wrapping_sub(0x3be) >= 0x43e - 0x3be {
        return x.powf(y); // x <= 0, subnormal, inf or nan; |y| < 2^-65 or >= 2^63
    }
    let (hi, lo) = log_inline(x.to_bits());
    let yhi = d(y.to_bits() & (u64::MAX << 27));
    let lhi = d(hi.to_bits() & (u64::MAX << 27));
    let llo = hi - lhi + lo;
    exp_inline(yhi * lhi, (y - yhi) * lhi + y * llo).unwrap_or_else(|| x.powf(y))
}
