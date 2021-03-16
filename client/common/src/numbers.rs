
pub struct NumberOp {
}

impl NumberOp {
    pub fn to_u128(id0: u64, id1: u64) -> u128 {
        let d : u128 = u128::from(id0) | u128::from(id1) << 64;
        d
    }

    pub fn from_u128(id: u128) -> Vec<u64> {
        let id1: u64 = (id>>64) as u64;
        let id0: u64 = ((id<<64) >> 64) as u64;
        let mut v: Vec<u64> = Vec::new();
        v.push(id0);
        v.push(id1);
        v
    }
}