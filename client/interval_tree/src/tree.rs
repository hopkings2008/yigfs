use std::rc::Rc;
use std::cell::RefCell;
use crate::tnode::TNode;

pub struct IntervalTree<T>{
    root: Option<Rc<RefCell<TNode<T>>>>,
}

impl<T> IntervalTree<T>{
    pub fn new() -> Self{
        IntervalTree{
            root: None,
        }
    }

    pub fn get(&self, start: u64, end: u64) -> Vec<Rc<RefCell<TNode<T>>>>{
        let mut v: Vec<Rc<RefCell<TNode<T>>>> = Vec::new();
        let mut x = self.root.clone();
        //println!("get: start: {}, end: {}", start, end);
        let mut depth = 0;
        loop {
            if x.is_none() {
                break;
            }
            depth += 1;
            let tmp_x = x.clone();
            let n = tmp_x.as_ref().unwrap();
            let nb = n.borrow();
            let intr = nb.get_intr();
            //println!("node: start: {}, end: {}, max: {}, for [{}, {})", intr.start, intr.end, nb.get_intr_end(), start, end);
            if intr.start <= start && start < intr.end {
                // got the start interval, need to track all the successors for the end.
                v.push(n.clone());
                if end <= intr.end {
                    break;
                }
                // end > intr.end, more than two intervals are needed.
                let mut curr = n.clone();
                loop {
                    let succ = self.successor(&curr);
                    if succ.is_none() {
                        break;
                    }
                    if succ.as_ref().unwrap().borrow().get_intr().start >= end {
                        break;
                    }

                    v.push(succ.as_ref().unwrap().clone());
                    
                    curr = succ.as_ref().unwrap().clone();
                }
                break;
            }

            // x's interval doesn't contain the [start, end)
            // x.left.end >= end
            if let Some(l) = n.borrow().get_lchild() {
                if l.borrow().get_intr_end() >= start {
                    x = n.borrow().get_lchild().clone();
                    continue;
                }
            }

            x = n.borrow().get_rchild().clone();
        }

        //println!("the depth of [{}, {}) is {}", start, end, depth);
        return v;
    }

    pub fn insert(&mut self, z: &Rc<RefCell<TNode<T>>>){
        let mut y: Option<Rc<RefCell<TNode<T>>>> = None;
        let mut x = self.root.clone();
        while x.is_some(){
            y = x.clone();
            if y.as_ref().unwrap().borrow().get_intr_end() < z.borrow().get_intr_end() {
                y.as_ref().unwrap().borrow_mut().set_intr_end(z.borrow().get_intr_end());
            }
            if z.borrow().get_key() <= x.as_ref().unwrap().borrow().get_key() {
                x = y.as_ref().unwrap().borrow().get_lchild().clone();
                continue;
            }
            x = y.as_ref().unwrap().borrow().get_rchild().clone();
        }
        
        // z.p = y
        z.borrow_mut().set_parent(&y);
        // if y == nil, root = z
        if y.is_none() {
            self.root = Some(z.clone());
        } else {
            if z.borrow().get_key() <= y.as_ref().unwrap().borrow().get_key() {
                y.as_ref().unwrap().borrow_mut().set_lchild(&Some(z.clone()));
            } else {
                y.as_ref().unwrap().borrow_mut().set_rchild(&Some(z.clone()));
            }
        }
        z.borrow_mut().set_lchild(&None);
        z.borrow_mut().set_rchild(&None);
        z.borrow_mut().set_color(1);

        //println!("insert: start: {}, end: {}", z.borrow().get_intr().start, z.borrow().get_intr().end);
        self.insert_fixup(z);
    }
    
    pub fn delete(&mut self, z: &Rc<RefCell<TNode<T>>>){
        // y stands for the deleted node z or z's successor
        let mut y = z.clone();
        let mut origin_color = y.borrow().get_color();
        // x stands for the y's successor
        let mut x: Option<Rc<RefCell<TNode<T>>>> = None;
        //let intr = z.borrow().get_intr();
        //println!("delete: intr: [{}, {})", intr.start, intr.end);
        if z.borrow().get_lchild().is_none() {
            x = z.borrow().get_rchild().clone();
            self.transplant(z, z.borrow().get_rchild());
        } else if z.borrow().get_rchild().is_none() {
            x = z.borrow().get_lchild().clone();
            self.transplant(z, z.borrow().get_lchild());
        } else {
            let min = self.tree_node_minum(z.borrow().get_rchild());
            if min.is_some(){
                y = min.unwrap().clone();
                origin_color = y.borrow().get_color();
                // y's left child must be nil.
                x = y.borrow().get_rchild().clone();
                if y.borrow().get_parent().as_ref().is_some() && y.borrow().get_parent().as_ref().unwrap().as_ptr() == z.as_ptr() {
                    if x.is_some() {
                        x.as_ref().unwrap().borrow_mut().set_parent(&Some(y.clone()));
                    }
                } else {
                    self.transplant(&y, y.borrow().get_rchild());
                    y.borrow_mut().set_rchild(z.borrow().get_rchild());
                    let yb = y.borrow();
                    let yr = yb.get_rchild();
                    if yr.is_some() {
                        yr.as_ref().unwrap().borrow_mut().set_parent(&Some(y.clone()));
                    }
                }
                self.transplant(z, &Some(y.clone()));
                y.borrow_mut().set_lchild(z.borrow().get_lchild());
                if y.borrow().get_lchild().is_some() {
                    y.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_parent(&Some(y.clone()));
                }
                y.borrow_mut().set_color(z.borrow().get_color());
            }
        }

        // adjust the max interval end for the ancestors of y.
        let mut adjust_node = y.clone();
        loop {
            let tmp_node = adjust_node.clone();
            let adjust_node_b = tmp_node.borrow();
            let p = adjust_node_b.get_parent();
            if p.is_none() {
                break;
            }
            let p_node = p.as_ref().unwrap();
            let end = p_node.borrow().get_intr().end;
            p_node.borrow_mut().set_intr_end(end);
            if p_node.borrow().get_lchild().is_some(){
                let l = p_node.borrow().get_lchild().as_ref().unwrap().clone();
                if l.borrow().get_intr_end() > p_node.borrow().get_intr_end() {
                    p_node.borrow_mut().set_intr_end(l.borrow().get_intr_end());
                }
            }
            if p_node.borrow().get_rchild().is_some() {
                let r = p_node.borrow().get_rchild().as_ref().unwrap().clone();
                if r.borrow().get_intr_end() > p_node.borrow().get_intr_end(){
                    p_node.borrow_mut().set_intr_end(r.borrow().get_intr_end());
                }
            }
            adjust_node = p_node.clone();
        }

        if origin_color == 0 {
            self.delete_fixup(&x);
        }
        // clear z's parent pointer and children's pointers.
        z.borrow_mut().set_parent(&None);
        z.borrow_mut().set_lchild(&None);
        z.borrow_mut().set_rchild(&None);
    }

    fn insert_fixup(&mut self, node: &Rc<RefCell<TNode<T>>>){
        let mut z = node.clone();
        while z.borrow().get_parent().is_some() &&  z.borrow().get_parent().as_ref().unwrap().borrow().get_color() == 1 {
            // z.p is nil or z.p.p is nil
            if z.borrow().get_parent().as_ref().unwrap().borrow().get_parent().is_none() {
                break;
            }
            // z.p != nil && z.p.p != nil
            let mut is_parent_left_child = false;
            if let Some(grandpa) = z.borrow().get_parent().as_ref().unwrap().borrow().get_parent(){
                if let Some(l) = grandpa.borrow().get_lchild() {
                    // z.p == z.p.p.left
                    if l.as_ptr() == z.borrow().get_parent().as_ref().unwrap().as_ptr() {
                        is_parent_left_child = true;
                    }
                }
            }
            if is_parent_left_child {
                let mut is_y_color_red = false;
                if let Some(y) = z.borrow().
                get_parent().as_ref().unwrap().borrow().get_parent().as_ref().unwrap().borrow().get_rchild() {
                    if y.borrow().get_color() == 1 {
                        is_y_color_red = true;
                    }
                }
                // y = z.p.p.right, if y.color == red
                if is_y_color_red {
                    // z.p.color = black
                    z.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(0);
                    // y.color = black
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                    z_p_p.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_color(0);
                    // z.p.p.color = red
                    z_p_p.borrow_mut().set_color(1);
                    // z = z.p.p
                    z = z_p_p.clone();
                } else if z.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().is_some() && 
                    z.as_ptr() == z.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().as_ptr() {
                    // z == z.p.right
                    // z = z.p
                    z = z.clone().borrow().get_parent().as_ref().unwrap().clone();
                    // left rotate on z
                    self.left_rotate(&z);
                }
                if z.borrow().get_parent().is_some(){
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    // z.p.color = black
                    z_p.borrow_mut().set_color(0);
                    if z_p.borrow().get_parent().is_some() {
                        let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                        // z.p.p.color = red
                        z_p_p.borrow_mut().set_color(1);
                        // right rotate on z.p.p
                        self.right_rotate(&z_p_p);
                    }
                }
            } else {
                let mut is_y_color_red = false;
                if let Some(y) = z.borrow().
                get_parent().as_ref().unwrap().borrow().get_parent().as_ref().unwrap().borrow().get_lchild() {
                    if y.borrow().get_color() == 1 {
                        is_y_color_red = true;
                    }
                }
                // y = z.p.p.left, if y.color == red
                if is_y_color_red {
                    // z.p.color = black
                    z.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(0);
                    // y.color = black
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                    z_p_p.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_color(0);
                    // z.p.p.color = red
                    z_p_p.borrow_mut().set_color(1);
                    // z = z.p.p
                    z = z_p_p.clone();
                } else if z.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().is_some() && 
                    z.as_ptr() == z.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().as_ptr() {
                    // z == z.p.left
                    // z = z.p
                    z = z.clone().borrow().get_parent().as_ref().unwrap().clone();
                    // right rotate on z
                    self.right_rotate(&z);
                }
                if z.borrow().get_parent().is_some(){
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    // z.p.color = black
                    z_p.borrow_mut().set_color(0);
                    if z_p.borrow().get_parent().is_some() {
                        let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                        // z.p.p.color = red
                        z_p_p.borrow_mut().set_color(1);
                        // left rotate on z.p.p
                        self.left_rotate(&z_p_p);
                    }
                }
            }
            
        }
        // T.root.color = black
        if self.root.is_some() {
            self.root.as_ref().unwrap().borrow_mut().set_color(0);
        }
    }

    fn left_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        // x doesn't have right child
        if x.borrow().get_rchild().is_none() {
            return;
        }
        // y = x.right
        let y = x.borrow().get_rchild().as_ref().unwrap().clone();
        // x.right = y.left
        x.borrow_mut().set_rchild(y.borrow().get_lchild());
        if y.borrow().get_lchild().is_some() {
            // y.left.parent = x
            y.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_parent(&Some(x.clone()));
        }
        // y.p = x.p
        y.borrow_mut().set_parent(x.borrow().get_parent());
        // if x.p == nil, T.root = y
        if x.borrow().get_parent().is_none() {
            self.root = Some(y.clone());
        } else if x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().is_some() &&
        x.as_ptr() == x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().as_ptr() {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_lchild(&Some(y.clone()));
        } else {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_rchild(&Some(y.clone()));
        }
        y.borrow_mut().set_lchild(&Some(x.clone()));
        x.borrow_mut().set_parent(&Some(y.clone()));
        //refresh the max interval end.
        // y.max = x.max
        y.borrow_mut().set_intr_end(x.borrow().get_intr_end());
        // x.max = max(x.end, x.left.max, x.right.max)
        let end = x.borrow().get_intr().end;
        x.borrow_mut().set_intr_end(end);
        if x.borrow().get_lchild().is_some(){
            let l = x.borrow().get_lchild().as_ref().unwrap().clone();
            if l.borrow().get_intr_end() > x.borrow().get_intr_end() {
                x.borrow_mut().set_intr_end(l.borrow().get_intr_end());
            }
        }
        if x.borrow().get_rchild().is_some(){
            let r = x.borrow().get_rchild().as_ref().unwrap().clone();
            if r.borrow().get_intr_end() > x.borrow().get_intr_end() {
                x.borrow_mut().set_intr_end(r.borrow().get_intr_end());
            }
        }
    }

    fn right_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        if x.borrow().get_lchild().is_none() {
            return;
        }
        // y = x.left
        let y = Rc::clone(x.borrow().get_lchild().as_ref().unwrap());
        
        // x.left = y.right
        x.borrow_mut().set_lchild(y.borrow().get_rchild());
        if y.borrow().get_rchild().is_some() {
            // y.right.parent = x
            y.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_parent(&Some(x.clone()));
        }
        // y.p = x.p
        y.borrow_mut().set_parent(x.borrow().get_parent());
        // if x.p == nil, T.root = y
        if x.borrow().get_parent().is_none() {
            self.root = Some(y.clone());
        } else if x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().is_some() &&
        x.as_ptr() == x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().as_ptr() {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_rchild(&Some(y.clone()));
        } else {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_lchild(&Some(y.clone()));
        }
        y.borrow_mut().set_rchild(&Some(x.clone()));
        x.borrow_mut().set_parent(&Some(y.clone()));

        //refresh the max interval end.
        // y.max = x.max
        y.borrow_mut().set_intr_end(x.borrow().get_intr_end());
        // x.max = max(x.end, x.left.max, x.right.max)
        let end = x.borrow().get_intr().end;
        x.borrow_mut().set_intr_end(end);
        if x.borrow().get_rchild().is_some(){
            let r = x.borrow().get_rchild().as_ref().unwrap().clone();
            if r.borrow().get_intr_end() > x.borrow().get_intr_end() {
                x.borrow_mut().set_intr_end(r.borrow().get_intr_end());
            }
        }
        if x.borrow().get_lchild().is_some(){
            let l = x.borrow().get_lchild().as_ref().unwrap().clone();
            if l.borrow().get_intr_end() > x.borrow().get_intr_end() {
                x.borrow_mut().set_intr_end(l.borrow().get_intr_end());
            }
        }
    }

    fn transplant(&mut self, u: &Rc<RefCell<TNode<T>>>, v: &Option<Rc<RefCell<TNode<T>>>>){
        if u.borrow().get_parent().is_none() {
            self.root = v.clone();
        } else {
            let ub = u.borrow();
            let p = ub.get_parent().as_ref().unwrap();
            if p.borrow().get_lchild().is_some() {
                let l = p.borrow().get_lchild().as_ref().unwrap().clone();
                if l.as_ptr() == u.as_ptr() {
                    p.borrow_mut().set_lchild(v);
                } else {
                    p.borrow_mut().set_rchild(v);
                }
            } else {// if p doesn't have left child, u must be p's right child.
                p.borrow_mut().set_rchild(v);
            };
        }
        if v.is_some() {
            v.as_ref().unwrap().borrow_mut().set_parent(u.borrow().get_parent());
        }
    }

    fn tree_node_minum(&self, n: &Option<Rc<RefCell<TNode<T>>>>) -> Option<Rc<RefCell<TNode<T>>>>{
        let mut x = n.clone();
        let mut y: Option<Rc<RefCell<TNode<T>>>> = None;
        while x.is_some() {
            y = x.clone();
            x = y.as_ref().unwrap().borrow().get_lchild().clone();
        }
        return y;
    }

    fn delete_fixup(&mut self, n: &Option<Rc<RefCell<TNode<T>>>>){
        if self.root.is_none() {
            return;
        }
        if n.is_none() {
            return;
        }

        let mut x = n.as_ref().unwrap().clone();
        //x != T:root and x:color == BLACK
        while x.as_ptr() != self.root.as_ref().unwrap().as_ptr() && 
            x.borrow().get_color() == 0 {
            // if x == x.p.left
            let mut is_x_p_left = false;
            if let Some(p) = x.borrow().get_parent() {
                if let Some(l) = p.borrow().get_lchild() {
                    if x.as_ptr() == l.as_ptr() {
                        is_x_p_left = true;
                    }
                }
            }
            if is_x_p_left {
                let tmp_x = x.clone();
                // w = x.p.right
                if let Some(mut w) = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().clone(){
                    // w.color == red
                    if w.borrow().get_color() == 1 {
                        // w.color = black
                        w.borrow_mut().set_color(0);
                        // x.p.color = red
                        x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(1);
                        // LEFT-ROTATE(T, x.p)
                        self.left_rotate(tmp_x.borrow().get_parent().as_ref().unwrap());
                        // w = x.p.right
                        w = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().clone();
                    }
                    // w.left.color == black and w.right.color == black
                    if w.borrow().get_lchild().is_some() && w.borrow().get_rchild().is_some() {
                        let wc = w.clone();
                        let wb = wc.borrow();
                        let l = wb.get_lchild().as_ref().unwrap();
                        let r = wb.get_rchild().as_ref().unwrap();
                        if l.borrow().get_color() == 0 && r.borrow().get_color() == 0 {
                            // w.color = red
                            w.borrow_mut().set_color(1);
                            x = tmp_x.borrow().get_parent().as_ref().unwrap().clone();
                        } else if r.borrow().get_color() == 0 {
                            // if w.r.color == black
                            // w.l.color = black
                            l.borrow_mut().set_color(0);
                            // w.color = red
                            w.borrow_mut().set_color(1);
                            // right rotation on w
                            self.right_rotate(&w);
                            // w = x.p.right
                            w = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().clone();
                        }
                    }
                    // w.color = x.p.color
                    w.borrow_mut().set_color(x.borrow().get_parent().as_ref().unwrap().borrow().get_color());
                    // x.p.color = black
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(0);
                    // w.right.color = black
                    w.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_color(0);
                    // left rotation on x.p
                    self.left_rotate(x.borrow().get_parent().as_ref().unwrap());
                };
                x = self.root.as_ref().unwrap().clone();
            } else {
                let tmp_x = x.clone();
                // w = x.p.left
                if let Some(mut w) = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().clone(){
                    // w.color == red
                    if w.borrow().get_color() == 1 {
                        // w.color = black
                        w.borrow_mut().set_color(0);
                        // x.p.color = red
                        x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(1);
                        // RIGHT-ROTATE(T, x.p)
                        self.right_rotate(tmp_x.borrow().get_parent().as_ref().unwrap());
                        // w = x.p.left
                        w = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().clone();
                    }
                    // w.left.color == black and w.right.color == black
                    if w.borrow().get_lchild().is_some() && w.borrow().get_rchild().is_some() {
                        let wc = w.clone();
                        let wb = wc.borrow();
                        let l = wb.get_lchild().as_ref().unwrap();
                        let r = wb.get_rchild().as_ref().unwrap();
                        if l.borrow().get_color() == 0 && r.borrow().get_color() == 0 {
                            // w.color = red
                            w.borrow_mut().set_color(1);
                            x = tmp_x.borrow().get_parent().as_ref().unwrap().clone();
                        } else if l.borrow().get_color() == 0 {
                            // if w.l.color == black
                            // w.r.color = black
                            r.borrow_mut().set_color(0);
                            // w.color = red
                            w.borrow_mut().set_color(1);
                            // left rotation on w
                            self.left_rotate(&w);
                            // w = x.p.left
                            w = tmp_x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().clone();
                        }
                    }
                    // w.color = x.p.color
                    w.borrow_mut().set_color(x.borrow().get_parent().as_ref().unwrap().borrow().get_color());
                    // x.p.color = black
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_color(0);
                    // w.left.color = black
                    w.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_color(0);
                    // right rotation on x.p
                    self.right_rotate(x.borrow().get_parent().as_ref().unwrap());
                };
                x = self.root.as_ref().unwrap().clone();
            }
        }
        // x.color = black
        x.borrow_mut().set_color(0);
    }

    fn successor(&self, n: &Rc<RefCell<TNode<T>>>) -> Option<Rc<RefCell<TNode<T>>>> {
        if n.borrow().get_rchild().is_some() {
            return self.tree_node_minum(n.borrow().get_rchild());
        }
        let mut y = n.borrow().get_parent().clone();
        let mut x = n.clone();
        // y != nil && x == y.right
        while let Some(node) = &y.clone() {
            if let Some(r) = node.borrow().get_rchild() {
                if x.as_ptr() == r.as_ptr() {
                    // x = y
                    x = node.clone();
                    // y = y.p
                    y = node.borrow().get_parent().clone();
                    continue;
                }
            }
            break;
        }

        return y;
    }
}