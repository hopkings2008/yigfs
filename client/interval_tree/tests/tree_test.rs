
use std::time::Instant;
use interval_tree::tree::IntervalTree;
use interval_tree::interval::Interval;

#[test]
fn test_interval_tree_basic_insert()->Result<(), String>{
    let mut tree = IntervalTree::new(Interval::new(0,0));
    
    let mut start = 0;
    let mut end = 10;
    loop{
        let intr = Interval::new(start, end);
        let n = tree.new_node(start, end, intr);
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
    let mut tree = IntervalTree::new(Interval::new(0,0));
    
    let mut start = 0;
    let mut end = 10;
    loop{
        let intr = Interval::new(start, end);
        let n = tree.new_node(start, end, intr);
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

#[test]
fn test_interval_tree_basic_delete()->Result<(), String>{
    let mut tree = IntervalTree::new(Interval::new(0,0));
    
    let mut start = 0;
    let mut end = 10;
    let mut count = 0;
    let limit = 1000;
    loop{
        let intr = Interval::new(start, end);
        let n = tree.new_node(start, end, intr);
        tree.insert(&n);
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    // remove intervals.
    start = 0;
    end = 10;
    count = 0;
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
        tree.delete(&nodes[0]);
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    // get the intervals again.
    start = 0;
    end = 10;
    count = 0;
    loop {
        let nodes = tree.get(start, end);
        if !nodes.is_empty() {
            for n in &nodes {
                let intr = n.borrow().get_intr();
                println!("after delete, got interval [{}, {})", intr.start, intr.end);
            }
            return Err(format!("get non-empty interval for start: {}, end: {}", start, end));
        }
        
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    Ok(())
}

#[test]
fn test_interval_tree_100w_get()->Result<(), String>{
    let mut tree = IntervalTree::new(Interval::new(0,0));
    
    let mut start = 0;
    let mut end = 10;
    let mut total_dur: u128 = 0;
    let mut count = 0;
    
    loop{
        let intr = Interval::new(start, end);
        let n = tree.new_node(start, end, intr);
        let begin = Instant::now();
        tree.insert(&n);
        let dur = begin.elapsed().as_nanos();
        total_dur += dur;
        start += 10;
        end += 10;
        count += 1;
        if count >= 1000000 {
            break;
        }
    }

    let average_dur = (total_dur as f64)/(count as f64);
    println!("insert average: {}, total_dur: {}", average_dur, total_dur);
    let root = tree.get_root();
    if root.is_none() {
        return Err(format!("empty root"));
    }
    let root_intr = root.as_ref().unwrap().borrow().get_intr();
    let root_intr_max = root.as_ref().unwrap().borrow().get_intr_end();
    println!("root is: [{}, {}), max: {}", root_intr.start, root_intr.end, root_intr_max);
    // get the intervals.
    start = 0;
    end = 10;
    count = 0;
    total_dur = 0;
    let limit = 1000;
    loop {
        let begin = Instant::now();
        let nodes = tree.get(start, end);
        let dur = begin.elapsed().as_nanos();
        total_dur += dur;
        count += 1;
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
        if count >= limit {
            break;
        }
    }

    let average_dur = (total_dur as f64)/(count as f64);
    println!("100w query: limit: {}, average query time: {}, total_dur: {}", count, average_dur, total_dur);

    Ok(())
}

#[test]
fn test_interval_tree_basic_traverse_free()->Result<(), String>{
    let mut tree = IntervalTree::new(0);
    
    let mut start = 0;
    let mut end = 10;
    let mut total_dur: u128 = 0;
    let mut count = 0;
    let limit = 100000;
    
    loop{
        let n = tree.new_node(start, end, start);
        let begin = Instant::now();
        tree.insert(&n);
        let dur = begin.elapsed().as_nanos();
        total_dur += dur;
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    let average_dur = (total_dur as f64)/(count as f64);
    println!("insert average: {}, total_dur: {}", average_dur, total_dur);
    let root = tree.get_root();
    if root.is_none() {
        return Err(format!("empty root"));
    }
    let root_intr = root.as_ref().unwrap().borrow().get_intr();
    let root_intr_max = root.as_ref().unwrap().borrow().get_intr_end();
    println!("root is: [{}, {}), max: {}", root_intr.start, root_intr.end, root_intr_max);
    // check the values of the tree.
    let begin = Instant::now();
    let values = tree.traverse_and_free();
    total_dur = begin.elapsed().as_nanos();
    if values.len() != limit as usize {
        return Err(format!("got {} values instead of {}", values.len(), limit));
    }
    println!("traverse_and_free {} elements consume dur: {}", limit, total_dur);
    count = 0;
    start = 0;
    loop {
        if values[count] != start {
            return Err(format!("index: {}, value is {} instead of {}", count, values[count], start));
        }
        count += 1;
        start += 10;
        if count >= limit {
            break;
        }
    }

    Ok(())
}

#[test]
fn test_interval_tree_basic_get_traverse()->Result<(), String>{
    let mut tree = IntervalTree::new(0);
    
    let mut start = 0;
    let mut end = 10;
    let mut total_dur: u128 = 0;
    let mut count = 0;
    let limit = 100000;
    
    loop{
        let n = tree.new_node(start, end, start);
        let begin = Instant::now();
        tree.insert(&n);
        let dur = begin.elapsed().as_nanos();
        total_dur += dur;
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    let average_dur = (total_dur as f64)/(count as f64);
    println!("insert average: {}, total_dur: {}", average_dur, total_dur);
    let root = tree.get_root();
    if root.is_none() {
        return Err(format!("empty root"));
    }
    let root_intr = root.as_ref().unwrap().borrow().get_intr();
    let root_intr_max = root.as_ref().unwrap().borrow().get_intr_end();
    println!("root is: [{}, {}), max: {}", root_intr.start, root_intr.end, root_intr_max);
    // check the values of the tree.
    let begin = Instant::now();
    let values = tree.traverse();
    total_dur = begin.elapsed().as_nanos();
    if values.len() != limit as usize {
        return Err(format!("got {} values instead of {}", values.len(), limit));
    }
    println!("traverse_and_free {} elements consume dur: {}", limit, total_dur);
    count = 0;
    start = 0;
    loop {
        if values[count] != start {
            return Err(format!("index: {}, value is {} instead of {}", count, values[count], start));
        }
        count += 1;
        start += 10;
        if count >= limit {
            break;
        }
    }

    Ok(())
}

#[test]
fn test_interval_tree_basic_vistor()->Result<(), String>{
    let mut tree = IntervalTree::new(0);
    
    let mut start = 0;
    let mut end = 10;
    let mut total_dur: u128 = 0;
    let mut count = 0;
    let limit = 100000;
    // set the version for the tree.
    tree.set_version(1);
    loop{
        let n = tree.new_node(start, end, start);
        let begin = Instant::now();
        tree.insert(&n);
        let dur = begin.elapsed().as_nanos();
        total_dur += dur;
        start += 10;
        end += 10;
        count += 1;
        if count >= limit {
            break;
        }
    }

    let average_dur = (total_dur as f64)/(count as f64);
    println!("insert average: {}, total_dur: {}", average_dur, total_dur);
    let root = tree.get_root();
    if root.is_none() {
        return Err(format!("empty root"));
    }
    let root_intr = root.as_ref().unwrap().borrow().get_intr();
    let root_intr_max = root.as_ref().unwrap().borrow().get_intr_end();
    println!("root is: [{}, {}), max: {}", root_intr.start, root_intr.end, root_intr_max);
    // check the values of the tree.
    let begin = Instant::now();
    let visitor = |version: u64 | -> bool {
        if version >= 1 {
            return true;
        }
        false
    };
    let values = tree.visitor(visitor);
    total_dur = begin.elapsed().as_nanos();
    if values.len() != limit as usize {
        return Err(format!("got {} values instead of {}", values.len(), limit));
    }
    println!("traverse_and_free {} elements consume dur: {}", limit, total_dur);
    count = 0;
    start = 0;
    loop {
        if values[count] != start {
            return Err(format!("index: {}, value is {} instead of {}", count, values[count], start));
        }
        count += 1;
        start += 10;
        if count >= limit {
            break;
        }
    }

    Ok(())
}