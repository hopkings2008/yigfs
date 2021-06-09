
use std::cell::RefCell;
use std::rc::Rc;

use interval_tree::tree::IntervalTree;
use interval_tree::tnode::TNode;
use interval_tree::interval::Interval;

#[test]
fn test_interval_tree_basic_insert()->Result<(), String>{
    let mut tree = IntervalTree::new();
    
    let mut start = 0;
    let mut end = 10;
    loop{
        let intr = Interval::new(start, end);
        let n = Rc::new(RefCell::new(TNode::<Interval>::new(start, end, intr)));
        tree.insert(&n);
        start += 10;
        end += 10;
        if start >= 10000 {
            break;
        }
    }

    // get the intervals.
    start = 0;
    end = 10;
    loop {
        let nodes = tree.get(start, end);
        if nodes.is_empty() {
            return Err(format!("get empty interval for start: {}, end: {}", start, end));
        }
        if nodes.len() > 1 {
            return Err(format!("got more than 1 interval for start: {}, end: {}, intervals: {:?}", start, end, nodes));
        }
        let intr = nodes[0].borrow().get_intr();
        if intr.start != start || intr.end != end {
            return Err(format!("got invalid interval: {:?} for start: {}, end: {}", intr, start, end));
        }
        start += 10;
        end += 10;
        if start >= 10000 {
            break;
        }
    }

    Ok(())
}