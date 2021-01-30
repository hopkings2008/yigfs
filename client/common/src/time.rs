extern crate time;

use time::Timespec;

pub fn nsecs_to_ts(nano: i64) -> Timespec{
    let sec = nano/1000000000;
    let nsec:i32 = (nano - sec as i64 * 1000000000) as i32;
    Timespec{
        sec: sec,
        nsec: nsec,
    }
}

pub fn ts_to_nsecs(ts: &Timespec) -> i64 {
    let sec = ts.sec * 1000000000;
    let nsec = ts.nsec as i64;
    sec + nsec
}