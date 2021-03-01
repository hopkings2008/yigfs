
pub struct NumberOp {
}

impl NumberOp {
    pub fn to_u128(id0: u64, id1: u64) -> u128 {
        let d : u128 = u128::from(id0) | u128::from(id1) << 64;
        d
    }
}