use std::rc::Rc;
use std::cell::RefCell;
use crate::tnode::TNode;

pub struct IntervalTree<T: Clone>{
    root: Option<Rc<RefCell<TNode<T>>>>,
    nil: Rc<RefCell<TNode<T>>>,
}

impl<T: Clone> IntervalTree<T>{
    pub fn new(val: T) -> Self{
        let nil = TNode::new_nil(val);
        
        IntervalTree{
            root: Some(nil.clone()),
            nil: nil.clone(),
        }
    }

    pub fn new_node(&self, start: u64, end: u64, val: T) -> Rc<RefCell<TNode<T>>>{
        let n = TNode::new(start, end, val, &self.nil);
        return Rc::new(RefCell::new(n));
    }

    pub fn get_root(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.root
    }

    pub fn get(&self, start: u64, end: u64) -> Vec<Rc<RefCell<TNode<T>>>>{
        let mut v: Vec<Rc<RefCell<TNode<T>>>> = Vec::new();
        let mut x = self.root.clone();

        loop {
            if x.as_ref().unwrap().borrow().is_nil() {
                break;
            }
            let tmp_x = x.clone();
            let n = tmp_x.as_ref().unwrap();
            let nb = n.borrow();
            let intr = nb.get_intr();
            //println!("node: color: {}, start: {}, end: {}, max: {}, for [{}, {})", nb.get_color_str(), intr.start, intr.end, nb.get_intr_end(), start, end);
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
                    if succ.as_ref().unwrap().borrow().is_nil() {
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
                if l.borrow().is_not_nil() && l.borrow().get_intr_end() >= start {
                    x = n.borrow().get_lchild().clone();
                    continue;
                }
            }

            x = n.borrow().get_rchild().clone();
        }

        return v;
    }

    pub fn insert(&mut self, z: &Rc<RefCell<TNode<T>>>){
        let mut y: Option<Rc<RefCell<TNode<T>>>> = Some(self.nil.clone());
        let mut x = self.root.clone();
        while x.as_ref().unwrap().borrow().is_not_nil(){
            y = x.clone();
            if x.as_ref().unwrap().borrow().get_intr_end() < z.borrow().get_intr_end() {
                x.as_ref().unwrap().borrow_mut().set_intr_end(z.borrow().get_intr_end());
            }
            if z.borrow().get_key() < x.as_ref().unwrap().borrow().get_key() {
                x = y.as_ref().unwrap().borrow().get_lchild().clone();
                continue;
            }
            x = y.as_ref().unwrap().borrow().get_rchild().clone();
        }
        
        // z.p = y
        z.borrow_mut().set_parent(y.clone());
        // if y == nil, root = z
        if y.as_ref().unwrap().borrow().is_nil() {
            self.root = Some(z.clone());
        } else {
            if z.borrow().get_key() < y.as_ref().unwrap().borrow().get_key() {
                y.as_ref().unwrap().borrow_mut().set_lchild(Some(z.clone()));
            } else {
                y.as_ref().unwrap().borrow_mut().set_rchild(Some(z.clone()));
            }
        }
        z.borrow_mut().set_lchild(Some(self.nil.clone()));
        z.borrow_mut().set_rchild(Some(self.nil.clone()));
        z.borrow_mut().set_red();

        //println!("insert: start: {}, end: {}", z.borrow().get_intr().start, z.borrow().get_intr().end);
        self.insert_fixup(z);
    }
    
    pub fn delete(&mut self, z: &Rc<RefCell<TNode<T>>>){
        // y stands for the deleted node z or z's successor
        let y = z.clone();
        let mut origin_color = y.borrow().get_color();
        // x stands for the y's successor
        let x: Option<Rc<RefCell<TNode<T>>>>;
        
        if z.borrow().get_lchild().as_ref().unwrap().borrow().is_nil() {
            x = z.borrow().get_rchild().clone();
            self.transplant(z, &z.borrow().get_rchild());
        } else if z.borrow().get_rchild().as_ref().unwrap().borrow().is_nil() {
            x = z.borrow().get_lchild().clone();
            self.transplant(z, &z.borrow().get_lchild());
        } else {
            let y = self.tree_node_minum(&z.borrow().get_rchild()).unwrap();
            origin_color = y.borrow().get_color();
            // y's left child must be nil.
            // x = y.right
            x = y.borrow().get_rchild().clone();
            if y.borrow().get_parent().as_ref().unwrap().as_ptr() == z.as_ptr() {
                // if y.p == z, x.p = y
                x.as_ref().unwrap().borrow_mut().set_parent(Some(y.clone()));
            } else {
                self.transplant(&y, &y.borrow().get_rchild());
                y.borrow_mut().set_rchild(z.borrow().get_rchild());
                y.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_parent(Some(y.clone()));
            }
            self.transplant(z, &Some(y.clone()));
            y.borrow_mut().set_lchild(z.borrow().get_lchild());
            y.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_parent(Some(y.clone()));
            y.borrow_mut().set_color(z.borrow().get_color());
            
        }

        // adjust the max interval end for the ancestors of y.
        let mut adjust_node = y.clone();
        while adjust_node.borrow().is_not_nil() {
            
            let end = adjust_node.borrow().get_intr().end;
            adjust_node.borrow_mut().set_intr_end(end);
            if adjust_node.borrow().get_lchild().as_ref().unwrap().borrow().is_not_nil(){
                let l = adjust_node.borrow().get_lchild().as_ref().unwrap().clone();
                if l.borrow().get_intr_end() > adjust_node.borrow().get_intr_end() {
                    adjust_node.borrow_mut().set_intr_end(l.borrow().get_intr_end());
                }
            }
            if adjust_node.borrow().get_rchild().as_ref().unwrap().borrow().is_not_nil() {
                let r = adjust_node.borrow().get_rchild().as_ref().unwrap().clone();
                if r.borrow().get_intr_end() > adjust_node.borrow().get_intr_end(){
                    adjust_node.borrow_mut().set_intr_end(r.borrow().get_intr_end());
                }
            }
            adjust_node = adjust_node.clone().borrow().get_parent().as_ref().unwrap().clone();
        }

        if origin_color.is_black() {
            self.delete_fixup(&x);
        }
        
        // free z.
        z.borrow_mut().set_parent(None);
        z.borrow_mut().set_lchild(None);
        z.borrow_mut().set_rchild(None);
        drop(z);
    }

    // traverse the tree in sequence and free it.
    pub fn traverse_and_free(&mut self) -> Vec<T> {
        let mut values: Vec<T> = Vec::new();
        let mut n = self.tree_node_minum(&self.root);
        // note: when access n, means that n's left children have already been accessed.
        // try to traverse the tree in mid sequence order.
        while n.as_ref().unwrap().borrow().is_not_nil() {
            values.push(n.as_ref().unwrap().borrow().get_value());
            if n.as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().borrow().is_not_nil() {
                n = self.tree_node_minum(&n.clone().as_ref().unwrap().borrow().get_rchild());
                continue;
            }
            // free the node if it doesn't have children.
            
            let mut p = n.as_ref().unwrap().borrow().get_parent().clone();
            loop {
                if p.as_ref().unwrap().borrow().is_nil() || n.as_ref().unwrap().as_ptr() == p.as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().as_ptr() {
                    self.free_node(&n);
                    n = p.clone();
                    if p.as_ref().unwrap().borrow().is_not_nil(){
                        p.as_ref().unwrap().borrow_mut().set_rchild(Some(self.nil.clone()));
                        p = p.clone().as_ref().unwrap().borrow().get_parent().clone();
                        continue;
                    }
                    break;
                }
                self.free_node(&n);
                p.as_ref().unwrap().borrow_mut().set_lchild(Some(self.nil.clone()));
                n = p;
                break;
            }
        }
        // set root the nil.
        self.root = Some(self.nil.clone());
        return values;
    }

    fn insert_fixup(&mut self, node: &Rc<RefCell<TNode<T>>>){
        let mut z = node.clone();
        // z.p.color == red
        while z.borrow().get_parent().as_ref().unwrap().borrow().is_red() {
            let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
            let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
            //z.p == z.p.p.left
            if z_p.as_ptr() == z_p_p.borrow().get_lchild().as_ref().unwrap().as_ptr() {
                // y = z.p.p.right, if y.color == red
                let y = z_p_p.borrow().get_rchild().as_ref().unwrap().clone();
                if y.borrow().is_red(){
                    // z.p.color = black
                    z_p.borrow_mut().set_black();
                    // y.color = black
                    y.borrow_mut().set_black();
                    // z.p.p.color = red
                    z_p_p.borrow_mut().set_red();
                    // z = z.p.p
                    z = z_p_p.clone();
                } else {
                    if z.as_ptr() == z_p.borrow().get_rchild().as_ref().unwrap().as_ptr() {
                        // if z == z.p.right
                        // z = z.p
                        z = z_p.clone();
                        // left rotate on z.p
                        self.left_rotate(&z);
                    }
                    // z.p.color = black.
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    z_p.borrow_mut().set_black();
                    // z.p.p.color = red
                    let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                    z_p_p.borrow_mut().set_red();
                    // right rotate on z.p.p
                    self.right_rotate(&z_p_p);  
                }
            } else { // z.p == z.p.p.right
                // y = z.p.p.left, if y.color == red
                let y = z_p_p.borrow().get_lchild().as_ref().unwrap().clone();
                if y.borrow().is_red(){
                    // z.p.color = black
                    z_p.borrow_mut().set_black();
                    // y.color = black
                    y.borrow_mut().set_black();
                    // z.p.p.color = red
                    z_p_p.borrow_mut().set_red();
                    // z = z.p.p
                    z = z_p_p.clone();
                } else {
                    if z.as_ptr() == z_p.borrow().get_lchild().as_ref().unwrap().as_ptr() {
                        // if z == z.p.left
                        // z = z.p
                        z = z_p.clone();
                        // right rotate on z.p
                        self.right_rotate(&z);
                    }
                    // z.p.color = black.
                    let z_p = z.borrow().get_parent().as_ref().unwrap().clone();
                    z_p.borrow_mut().set_black();
                    // z.p.p.color = red
                    let z_p_p = z_p.borrow().get_parent().as_ref().unwrap().clone();
                    z_p_p.borrow_mut().set_red();
                    // left rotate on z.p.p
                    self.left_rotate(&z_p_p);
                }
            }
            
        }
        // T.root.color = black
        self.root.as_ref().unwrap().borrow_mut().set_black();
    }

    fn left_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        // y = x.right
        let y = x.borrow().get_rchild().as_ref().unwrap().clone();
        // x.right = y.left
        x.borrow_mut().set_rchild(y.borrow().get_lchild());
        // if y.left != nil, y.left.parent = x
        if y.borrow().get_lchild().as_ref().unwrap().borrow().is_not_nil(){
            y.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_parent(Some(x.clone()));
        }
        
        // y.p = x.p
        y.borrow_mut().set_parent(x.borrow().get_parent());
        // if x.p == nil, T.root = y
        if x.borrow().get_parent().as_ref().unwrap().borrow().is_nil() {
            self.root = Some(y.clone());
        } else if x.as_ptr() == x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().as_ptr() {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_lchild(Some(y.clone()));
        } else {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_rchild(Some(y.clone()));
        }
        // y.left = x
        y.borrow_mut().set_lchild(Some(x.clone()));
        // x.parent = y
        x.borrow_mut().set_parent(Some(y.clone()));

        //refresh the max interval end.
        // y.max = x.max
        y.borrow_mut().set_intr_end(x.borrow().get_intr_end());
        // x.max = max(x.end, x.left.max, x.right.max)
        let end = x.borrow().get_intr().end;
        x.borrow_mut().set_intr_end(end);
        let lmax = x.borrow().get_lchild().as_ref().unwrap().borrow().get_intr_end();
        if lmax > x.borrow().get_intr_end() {
            x.borrow_mut().set_intr_end(lmax);
        }
        let rmax = x.borrow().get_rchild().as_ref().unwrap().borrow().get_intr_end();
        if rmax > x.borrow().get_intr_end() {
            x.borrow_mut().set_intr_end(rmax);
        }
    }

    fn right_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        // y = x.left
        let y = Rc::clone(x.borrow().get_lchild().as_ref().unwrap());
        
        // x.left = y.right
        x.borrow_mut().set_lchild(y.borrow().get_rchild());
        // y.right.parent = x
        y.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_parent(Some(x.clone()));
        // y.p = x.p
        y.borrow_mut().set_parent(x.borrow().get_parent());
        // if x.p == nil, T.root = y
        if x.borrow().get_parent().as_ref().unwrap().borrow().is_nil() {
            self.root = Some(y.clone());
        } else if x.as_ptr() == x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().as_ptr() {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_rchild(Some(y.clone()));
        } else {
            x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_lchild(Some(y.clone()));
        }
        // y.right = x
        y.borrow_mut().set_rchild(Some(x.clone()));
        // x.parent = y
        x.borrow_mut().set_parent(Some(y.clone()));

        //refresh the max interval end.
        // y.max = x.max
        y.borrow_mut().set_intr_end(x.borrow().get_intr_end());
        // x.max = max(x.end, x.left.max, x.right.max)
        let end = x.borrow().get_intr().end;
        x.borrow_mut().set_intr_end(end);
        
        let rmax = x.borrow().get_rchild().as_ref().unwrap().borrow().get_intr_end();
        if rmax > x.borrow().get_intr_end() {
            x.borrow_mut().set_intr_end(rmax);
        }

        
        let lmax = x.borrow().get_lchild().as_ref().unwrap().borrow().get_intr_end();
        if lmax > x.borrow().get_intr_end() {
            x.borrow_mut().set_intr_end(lmax);
        }
    }

    fn transplant(&mut self, u: &Rc<RefCell<TNode<T>>>, v: &Option<Rc<RefCell<TNode<T>>>>){
        if u.borrow().get_parent().as_ref().unwrap().borrow().is_nil() {
            self.root = v.clone();
        } else if u.as_ptr() == u.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().as_ptr(){
            u.borrow().get_parent().as_ref().unwrap().borrow_mut().set_lchild(v.clone());
        } else {
            u.borrow().get_parent().as_ref().unwrap().borrow_mut().set_rchild(v.clone());
        }
        
        v.as_ref().unwrap().borrow_mut().set_parent(u.borrow().get_parent());
    }

    fn tree_node_minum(&self, n: &Option<Rc<RefCell<TNode<T>>>>) -> Option<Rc<RefCell<TNode<T>>>>{
        let mut x = n.clone();
        let mut y: Option<Rc<RefCell<TNode<T>>>> = Some(self.nil.clone());
        while x.as_ref().unwrap().borrow().is_not_nil() {
            y = x.clone();
            x = y.as_ref().unwrap().borrow().get_lchild().clone();
        }
        return y;
    }

    fn delete_fixup(&mut self, n: &Option<Rc<RefCell<TNode<T>>>>){
        let mut x = n.as_ref().unwrap().clone();
        if x.borrow().is_nil() {
            return;
        }
        //x != T:root and x:color == BLACK
        while x.as_ptr() != self.root.as_ref().unwrap().as_ptr() && 
            x.borrow().is_black() {
            // if x == x.p.left
            if x.as_ptr() == x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().as_ptr() {
                // w = x.p.right
                let mut w = x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().clone();
                // w.color == red
                if w.borrow().is_red() {
                    // w.color = black
                    w.borrow_mut().set_black();
                    // x.p.color = red
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_red();
                    // LEFT-ROTATE(T, x.p)
                    self.left_rotate(x.borrow().get_parent().as_ref().unwrap());
                    // w = x.p.right
                    w = x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().clone();
                }
                // w.left.color == black and w.right.color == black
                let l = w.borrow().get_lchild().as_ref().unwrap().clone();
                let r = w.borrow().get_rchild().as_ref().unwrap().clone();
                if l.borrow().is_black() && r.borrow().is_black() {
                    // w.color = red
                    w.borrow_mut().set_red();
                    // x = x.p
                    x = x.clone().borrow().get_parent().as_ref().unwrap().clone();
                } else {
                    if r.borrow().is_black() {
                        // if w.r.color == black
                        // w.l.color = black
                        l.borrow_mut().set_black();
                        // w.color = red
                        w.borrow_mut().set_red();
                        // right rotation on w
                        self.right_rotate(&w);
                        // w = x.p.right
                        w = x.borrow().get_parent().as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().clone();
                    }
                
                    // w.color = x.p.color
                    w.borrow_mut().set_color(x.borrow().get_parent().as_ref().unwrap().borrow().get_color());
                    // x.p.color = black
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_black();
                    // w.right.color = black
                    w.borrow().get_rchild().as_ref().unwrap().borrow_mut().set_black();
                    // left rotation on x.p
                    self.left_rotate(x.borrow().get_parent().as_ref().unwrap());
                    // x = T.root
                    x = self.root.as_ref().unwrap().clone();
                }
            } else {
                // w = x.p.left
                let mut w = x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().clone();
                // w.color == red
                if w.borrow().is_red() {
                    // w.color = black
                    w.borrow_mut().set_black();
                    // x.p.color = red
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_red();
                    // RIGHT-ROTATE(T, x.p)
                    self.right_rotate(x.borrow().get_parent().as_ref().unwrap());
                    // w = x.p.left
                    w = x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().clone();
                }
                // w.left.color == black and w.right.color == black
                let l = w.borrow().get_lchild().as_ref().unwrap().clone();
                let r = w.borrow().get_rchild().as_ref().unwrap().clone();
                if l.borrow().is_black() && r.borrow().is_black() {
                    // w.color = red
                    w.borrow_mut().set_red();
                    // x = x.p
                    x = x.clone().borrow().get_parent().as_ref().unwrap().clone();
                } else {
                    if l.borrow().is_black() {
                        // if w.l.color == black
                        // w.r.color = black
                        r.borrow_mut().set_black();
                        // w.color = red
                        w.borrow_mut().set_red();
                        // left rotation on w
                        self.left_rotate(&w);
                        // w = x.p.left
                        w = x.borrow().get_parent().as_ref().unwrap().borrow().get_lchild().as_ref().unwrap().clone();
                    }
                
                    // w.color = x.p.color
                    w.borrow_mut().set_color(x.borrow().get_parent().as_ref().unwrap().borrow().get_color());
                    // x.p.color = black
                    x.borrow().get_parent().as_ref().unwrap().borrow_mut().set_black();
                    // w.left.color = black
                    w.borrow().get_lchild().as_ref().unwrap().borrow_mut().set_black();
                    // right rotation on x.p
                    self.right_rotate(x.borrow().get_parent().as_ref().unwrap());
                    
                    x = self.root.as_ref().unwrap().clone();
                }
            }
        }
        // x.color = black
        x.borrow_mut().set_black();
    }

    fn successor(&self, n: &Rc<RefCell<TNode<T>>>) -> Option<Rc<RefCell<TNode<T>>>> {
        if n.borrow().get_rchild().as_ref().unwrap().borrow().is_not_nil() {
            return self.tree_node_minum(&n.borrow().get_rchild());
        }
        let mut y = n.borrow().get_parent().clone();
        let mut x = n.clone();
        // y != nil && x == y.right
        while y.as_ref().unwrap().borrow().is_not_nil() &&
        x.as_ptr() == y.as_ref().unwrap().borrow().get_rchild().as_ref().unwrap().as_ptr() {
            // x = y
            x = y.as_ref().unwrap().clone();
            // y = y.p
            y = y.clone().as_ref().unwrap().borrow().get_parent().clone();
        }

        return y;
    }

    fn free_node(&self, n: &Option<Rc<RefCell<TNode<T>>>>) {
        if n.as_ref().unwrap().borrow().is_nil() {
            return;
        }
        // clear the pointers of this node.
        n.as_ref().unwrap().borrow_mut().set_parent(None);
        n.as_ref().unwrap().borrow_mut().set_lchild(None);
        n.as_ref().unwrap().borrow_mut().set_rchild(None);
        drop(n);
    }
}