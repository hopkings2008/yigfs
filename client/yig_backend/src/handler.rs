
use crate::event::IoEventResult;

pub trait CompleteHandler{
    // should contain inner context, and the function should not return anything.
    fn handle(&self, event: IoEventResult);
}