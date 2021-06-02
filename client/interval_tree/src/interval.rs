pub struct Interval {
    pub start: u64,
    pub end: u64,
}

impl Interval {
    pub fn new(start: u64, end: u64) -> Self{
        Interval{
            start: start,
            end: end,
        }
    }
}