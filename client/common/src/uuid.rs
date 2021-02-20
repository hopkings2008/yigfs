extern crate uuid;
extern crate hex;
use uuid::Uuid;
use hex::encode;

pub fn uuid_string()-> String {
    let id = Uuid::new_v4();
    let bytes = id.as_bytes();
    encode(bytes)
}

// generate two u64 from uuid in little endian.
pub fn uuid_u64_le() -> Vec<u64>{
    let id = Uuid::new_v4();
    let bytes = id.as_bytes();
    let u0 = u64::from(bytes[0]) |
        u64::from(bytes[1]) << 8 |
        u64::from(bytes[2]) << 16 |
        u64::from(bytes[3]) << 24 |
        u64::from(bytes[4]) << 32 |
        u64::from(bytes[5]) << 40 |
        u64::from(bytes[6]) << 48 |
        u64::from(bytes[7]) << 56 ;
    let u1 = u64::from(bytes[8]) |
        u64::from(bytes[9]) << 8 |
        u64::from(bytes[10]) << 16 |
        u64::from(bytes[11]) << 24 |
        u64::from(bytes[12]) << 32 |
        u64::from(bytes[13]) << 40 |
        u64::from(bytes[14]) << 48 |
        u64::from(bytes[15]) << 56 ;
    let mut ret : Vec<u64> = Vec::new();
    ret.push(u0);
    ret.push(u1);
    return ret;
}