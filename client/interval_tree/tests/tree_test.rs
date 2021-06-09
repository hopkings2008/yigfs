
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

#[test]
fn test_interval_tree_basic_get_range()->Result<(), String>{
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

    // check [5,20), should return [0,10), [10,20)
    start = 5;
    end = 20;
    let nodes = tree.get(start, end);
    if nodes.is_empty() {
        return Err(format!("got empty nodes for [{}, {})", start, end));
    }

    for n in &nodes {
        let intr = n.borrow().get_intr();
        println!("got intr: [{}, {})", intr.start, intr.end);
    }

    if nodes.len() > 2 {
        return Err(format!("got more than 2 nodes for [{}, {})", start, end));
    }

    let intr = nodes[0].borrow().get_intr();
    if intr.start != 0 || intr.end != 10 {
        return Err(format!("got invalid 1st interval[{}, {}) for [{}, {})", intr.start, intr.end, 0, 10));
    }

    let intr = nodes[1].borrow().get_intr();
    if intr.start != 10 || intr.end != 20 {
        return Err(format!("got invalid 2st interval[{}, {}) for [10, 20)", intr.start, intr.end));
    }

    return Ok(());
}