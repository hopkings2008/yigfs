use std::rc::Rc;
use std::cell::RefCell;
use crate::interval::Interval;

pub struct TNode<T>{
    p: Option<Rc<RefCell<TNode<T>>>>,
    l: Option<Rc<RefCell<TNode<T>>>>,
    r: Option<Rc<RefCell<TNode<T>>>>,
    color: u8, // 0 for black, 1 for red.
    key: u64,
    intr: Interval,
    intr_end: u64,
    val: T,
}

impl<T> TNode<T>{
    //[start, end)
    pub fn new(start: u64, end: u64, val: T, color: u8) -> Self{
        TNode{
            p: None,
            l: None,
            r: None,
            color: color,
            key: start,
            intr: Interval::new(start, end),
            intr_end: end,
            val: val,
        }
    }

    pub fn set_lchild(&mut self, l: &Option<Rc<RefCell<TNode<T>>>>) {
        match l {
            Some(n) => {
                self.l = Some(n.clone());
            }
            None => {
                self.l = None;
            }
        }
    }

    pub fn set_rchild(&mut self, r: &Option<Rc<RefCell<TNode<T>>>>) {
        match r{
            Some(n) => {
                self.r = Some(n.clone());
            }
            None => {
                self.r = None;
            }
        }
    }

    pub fn get_lchild(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.l
    }

    pub fn get_rchild(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.r
    }

    pub fn set_parent(&mut self, p: &Option<Rc<RefCell<TNode<T>>>>) {
        match p {
            Some(p) => {
                self.p = Some(p.clone());
            }
            None => {
                self.p = None;
            }
        }
    }

    pub fn get_parent(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.p
    }

    pub fn get_color(&self) -> u8 {
        self.color
    }

    pub fn set_color(&mut self, color: u8) {
        self.color = color;
    }

    pub fn get_intr(&self) -> &Interval {
        &self.intr
    }

    pub fn get_intr_end(&self) -> u64{
        self.intr_end
    }

    pub fn set_intr_end(&mut self, end: u64) {
        self.intr_end = end;
    }

    pub fn set_key(&mut self, key: u64) {
        self.key = key;
    }

    pub fn get_key(&self) -> u64{
        self.key
    }
}