
use std::cell::RefCell;
use std::rc::Rc;

use interval_tree::tree::IntervalTree;
use interval_tree::tnode::TNode;
use interval_tree::interval::Interval;

#[test]
fn interval_tree_basic_insert(){
    let mut tree = IntervalTree::new();
    
    let mut start = 0;
    let mut end = 10;
    loop{
        let intr = Interval::new(start, end);
        let n = Rc::new(RefCell::new(TNode::<Interval>::new(start, end, intr)));
        tree.insert(&n);
        start += 10;
        end += 10;
        if start >= 100 {
            break;
        }
    }
}