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
    pub fn new(start: u64, end: u64, val: T) -> Self{
        TNode{
            p: None,
            l: None,
            r: None,
            color: 1, // initial the node to red
            key: start,
            intr: Interval::new(start, end),
            intr_end: end,
            val: val,
        }
    }

    pub fn set_lchild(&mut self, l: &Option<Rc<RefCell<TNode<T>>>>) {
        self.l = l.clone();
    }

    pub fn set_rchild(&mut self, r: &Option<Rc<RefCell<TNode<T>>>>) {
        self.r = r.clone();
    }

    pub fn get_lchild(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.l
    }

    pub fn get_rchild(&self) -> &Option<Rc<RefCell<TNode<T>>>> {
        &self.r
    }

    pub fn get_num_of_children(&self) -> u32 {
        let mut num = 0;
        if self.l.is_some() {
            num += 1;
        }
        if self.r.is_some() {
            num += 1;
        }

        num
    }

    pub fn set_parent(&mut self, p: &Option<Rc<RefCell<TNode<T>>>>) {
        self.p = p.clone();
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