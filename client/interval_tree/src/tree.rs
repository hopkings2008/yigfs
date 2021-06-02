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

    fn insert(&mut self, z: &Rc<RefCell<TNode<T>>>){
        let mut y: Option<Rc<RefCell<TNode<T>>>> = None;
        let mut x = self.root.clone();
        while x.is_some(){
            let tmp = x.clone();
            let n:&Rc<RefCell<TNode<T>>> = tmp.as_ref().unwrap();
            y = x.clone();
            if z.borrow().get_key() < n.borrow().get_key() {
                x = n.borrow().get_lchild().clone();
                continue;
            }
            x = n.borrow().get_rchild().clone();
        }
        // z.p = y
        z.borrow_mut().set_parent(&y);
        // if y == nil, root = z
        if y.is_none() {
            self.root = Some(z.clone());
        } else {
            let n = y.as_ref().unwrap();
            if z.borrow().get_key() < n.borrow().get_key() {
                n.borrow_mut().set_lchild(&Some(z.clone()));
            } else {
                n.borrow_mut().set_rchild(&Some(z.clone()));
            }
        }
        z.borrow_mut().set_lchild(&None);
        z.borrow_mut().set_rchild(&None);
        z.borrow_mut().set_color(1);
        self.insert_fixup(z);
    }

    fn insert_fixup(&mut self, node: &Rc<RefCell<TNode<T>>>){
        let mut z = node.clone();
        while z.borrow().get_color() == 1 {
            // parent & grandpa don't exists.
            if z.borrow().get_parent().is_none() || z.borrow().get_parent().as_ref().unwrap().borrow().get_parent().is_none() {
                break;
            }
            let tmp = z.clone();
            let tz = tmp.borrow_mut();
            // z.p
            let z_p = tz.get_parent().as_ref().unwrap();
            let mut z_p_m = z_p.borrow_mut();
            let pzt = z_p.borrow();
            // z.p.p
            let z_p_p = pzt.get_parent().as_ref().unwrap();
            
            // if z.p == z.p.p.left
            let mut is_parent_left_child = false;
            if let Some(ppl) = z_p_p.borrow().get_lchild() {
                if ppl.as_ptr() == z_p.as_ptr() {
                    is_parent_left_child = true;
                }
            }
            
            if is_parent_left_child {
                // if y.color == red.
                let mut is_y_color_red = false;
                if let Some(y) = z_p_p.borrow().get_rchild() {
                    if y.borrow().get_color() == 1 {
                        // y.color = black.
                        y.borrow_mut().set_color(0);
                        is_y_color_red = true;
                    }
                }
                if is_y_color_red {
                    // z.p.color = black.
                    z_p_m.set_color(0);
                    // z.p.p.color = red.
                    z_p_p.borrow_mut().set_color(1);
                    // z = z.p.p
                    z = tmp.borrow_mut().get_parent().as_ref().unwrap().borrow_mut().get_parent().as_ref().unwrap().clone();
                } else if let Some(zpl) = z_p.borrow().get_rchild() {
                    // if z == z.p.right
                    if zpl.as_ptr() == z.as_ptr() {
                        // z = z.p
                        z = tmp.borrow_mut().get_parent().as_ref().unwrap().clone();
                        // perform left rotation.
                        self.left_rotate(&z);
                    }
                }
                if z.borrow().get_parent().is_some() && z.borrow().get_parent().as_ref().unwrap().borrow().get_parent().is_some(){
                    let z_b = z.borrow();
                    let z_p = z_b.get_parent().as_ref().unwrap();
                    let z_p_b = z_p.borrow();
                    let z_p_p = z_p_b.get_parent().as_ref().unwrap();
                    // z.p.color = black.
                    z_p.borrow_mut().set_color(0);
                    // z.p.p.color = red.
                    z_p_p.borrow_mut().set_color(1);
                    // perform right rotation.
                    self.right_rotate(z_p_p);
                }
            } else {
                // must be the right child, since z_p exists, z_p_p has at least one child.
                // if y.color == red.
                let mut is_y_color_red = false;
                if let Some(y) = z_p_p.borrow().get_lchild() {
                    if y.borrow().get_color() == 1 {
                        // y.color = black.
                        y.borrow_mut().set_color(0);
                        is_y_color_red = true;
                    }
                }
                if is_y_color_red {
                    // z.p.color = black.
                    z_p_m.set_color(0);
                    // z.p.p.color = red.
                    z_p_p.borrow_mut().set_color(1);
                    // z = z.p.p
                    z = tmp.borrow_mut().get_parent().as_ref().unwrap().borrow_mut().get_parent().as_ref().unwrap().clone();
                } else if let Some(zpl) = z_p.borrow().get_lchild() {
                    // if z == z.p.left
                    if zpl.as_ptr() == z.as_ptr() {
                        // z = z.p
                        z = tmp.borrow_mut().get_parent().as_ref().unwrap().clone();
                        // perform left rotation.
                        self.right_rotate(&z);
                    }
                }
                if z.borrow().get_parent().is_some() && z.borrow().get_parent().as_ref().unwrap().borrow().get_parent().is_some(){
                    let z_b = z.borrow();
                    let z_p = z_b.get_parent().as_ref().unwrap();
                    let z_p_b = z_p.borrow();
                    let z_p_p = z_p_b.get_parent().as_ref().unwrap();
                    // z.p.color = black.
                    z_p.borrow_mut().set_color(0);
                    // z.p.p.color = red.
                    z_p_p.borrow_mut().set_color(1);
                    // perform left rotation.
                    self.left_rotate(z_p_p);
                }
            }
        }
        if let Some(root) = &self.root {
            root.borrow_mut().set_color(0);
        }
    }

    fn left_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        let t = RefCell::borrow(x);
        let y = t.get_rchild();
        match y {
            Some(y) => {
                x.borrow_mut().set_rchild(y.borrow().get_lchild());
                if let Some(r) = y.borrow().get_lchild() {
                    r.borrow_mut().set_parent(&Some(x.clone()));
                }
                y.borrow_mut().set_parent(x.borrow().get_parent());
                match x.borrow_mut().get_parent(){
                    Some(p) => {
                        if let Some(l) = p.borrow().get_lchild() {
                            if x.as_ptr() == l.as_ptr() {
                                p.borrow_mut().set_lchild(&Some(y.clone()));
                            }
                        } else if let Some(r) = p.borrow().get_rchild(){
                            if x.as_ptr() == r.as_ptr() {
                                p.borrow_mut().set_rchild(&Some(y.clone()));
                            }
                        }
                    }
                    None => {
                        self.root = Some(y.clone());
                    }
                }
                y.borrow_mut().set_lchild(&Some(x.clone()));
                x.borrow_mut().set_parent(&Some(y.clone()));
            }
            None => {
                // since x doesn't have right child, we cannot perform left rotation.
                return;
            }
        }
    }

    fn right_rotate(&mut self, x: &Rc<RefCell<TNode<T>>>) {
        let t = RefCell::borrow(x);
        let y = t.get_lchild();
        match y {
            Some(y) => {
                x.borrow_mut().set_lchild(y.borrow().get_rchild());
                if let Some(r) = y.borrow().get_rchild() {
                    r.borrow_mut().set_parent(&Some(x.clone()));
                }
                y.borrow_mut().set_parent(x.borrow().get_parent());
                match x.borrow_mut().get_parent(){
                    Some(p) => {
                        if let Some(l) = p.borrow().get_lchild() {
                            if x.as_ptr() == l.as_ptr() {
                                p.borrow_mut().set_lchild(&Some(y.clone()));
                            }
                        } else if let Some(r) = p.borrow().get_rchild(){
                            if x.as_ptr() == r.as_ptr() {
                                p.borrow_mut().set_rchild(&Some(y.clone()));
                            }
                        }
                    }
                    None => {
                        self.root = Some(y.clone());
                    }
                }
                y.borrow_mut().set_rchild(&Some(x.clone()));
                x.borrow_mut().set_parent(&Some(y.clone()));
            }
            None => {
                // since x doesn't have left child, we cannot perform right rotation.
                return;
            }
        }
    }
}