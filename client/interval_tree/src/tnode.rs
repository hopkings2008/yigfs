use std::rc::Rc;
use std::cell::RefCell;
use crate::interval::Interval;


#[derive(Debug, Clone)]
pub enum NodeFlag {
    Nil,
}

impl NodeFlag {
    pub fn is_nil (&self) -> bool {
        match *self {
            Self::Nil => {
                true
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Color {
    BLACK = 0,
    RED
}

impl Color {
    pub fn is_black(&self) -> bool {
        match *self {
            Self::BLACK => {
                true
            }
            _ => {
                false
            }
        }
    }

    pub fn is_red(&self) -> bool {
        match *self {
            Self::RED => {
                true
            }
            _ => {
                false
            }
        }
    }

    pub fn to_str(&self) -> String {
        match *self {
            Self::BLACK => {
                String::from("black")
            }
            Self::RED => {
                String::from("red")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TNode<T: Clone>{
    p: Option<Rc<RefCell<TNode<T>>>>,
    l: Option<Rc<RefCell<TNode<T>>>>,
    r: Option<Rc<RefCell<TNode<T>>>>,
    flag: Option<NodeFlag>,
    color: Color, // 0 for black, 1 for red.
    key: u64,
    intr: Interval,
    intr_end: u64,
    val: T,
}

impl<T:Clone> TNode<T>{
    //[start, end)
    pub fn new(start: u64, end: u64, val: T, nil: &Rc<RefCell<TNode<T>>>) -> Self{
        TNode{
            p: Some(nil.clone()),
            l: Some(nil.clone()),
            r: Some(nil.clone()),
            flag: None,
            color: Color::RED, // initial the node to red
            key: start,
            intr: Interval::new(start, end),
            intr_end: end,
            val: val,
        }
    }

    pub fn new_nil(val: T)->Rc<RefCell<TNode<T>>> {
        let n = Rc::new(RefCell::new(TNode{
            p: None,
            l: None,
            r: None,
            flag: Some(NodeFlag::Nil),
            color: Color::BLACK,
            key: 0,
            intr: Interval::new(0,0),
            intr_end: 0,
            val: val,
        }));
        return n;
    }

    pub fn is_nil(&self) -> bool {
        if let Some(flag) = &self.flag {
            return flag.is_nil();
        }
        return false;
    }

    pub fn is_not_nil(&self) -> bool {
        !self.is_nil()
    }

    pub fn set_lchild(&mut self, l: Option<Rc<RefCell<TNode<T>>>>) {
        if self.is_not_nil() {
            self.l = l;
        }
    }

    pub fn set_rchild(&mut self, r: Option<Rc<RefCell<TNode<T>>>>) {
        if self.is_not_nil(){
            self.r = r;
        }
    }

    pub fn get_lchild(&self) -> Option<Rc<RefCell<TNode<T>>>> {
        if self.is_nil() {
            return Some(Rc::new(RefCell::new((*self).clone())));
        }
        return self.l.clone()
    }

    pub fn get_rchild(&self) -> Option<Rc<RefCell<TNode<T>>>> {
        if self.is_nil() {
            return Some(Rc::new(RefCell::new((*self).clone())));
        }
        return self.r.clone()
    }

    pub fn get_num_of_children(&self) -> u32 {
        let mut num = 0;
        if self.l.as_ref().unwrap().borrow().is_not_nil(){
            num += 1;
        }
        if self.r.as_ref().unwrap().borrow().is_not_nil() {
            num += 1;
        }

        num
    }

    pub fn set_parent(&mut self, p: Option<Rc<RefCell<TNode<T>>>>) {
        if self.is_not_nil(){
            self.p = p;
        }
    }

    pub fn get_parent(&self) -> Option<Rc<RefCell<TNode<T>>>> {
        if self.is_nil() {
            return Some(Rc::new(RefCell::new((*self).clone())));
        }
        return self.p.clone();
    }

    pub fn get_color(&self) -> Color {
        self.color.clone()
    }

    pub fn set_color(&mut self, color: Color){
        if self.is_not_nil(){
            self.color = color;
        }
    }

    pub fn get_color_str(&self) -> String {
        self.color.to_str()
    }

    pub fn is_red(&self) -> bool {
        self.color.is_red()
    }

    pub fn is_black(&self) -> bool {
        self.color.is_black()
    }

    pub fn set_red(&mut self) {
        if self.is_not_nil(){
            self.color = Color::RED;
        }
    }

    pub fn set_black(&mut self) {
        if self.is_not_nil(){
            self.color = Color::BLACK;
        }
    }

    pub fn get_intr(&self) -> Interval {
        Interval {
            start: self.intr.start,
            end: self.intr.end,
        }
    }

    pub fn get_intr_end(&self) -> u64{
        self.intr_end
    }

    pub fn set_intr_end(&mut self, end: u64) {
        if self.is_not_nil(){
            self.intr_end = end;
        }
    }

    pub fn set_key(&mut self, key: u64) {
        if self.is_not_nil(){
            self.key = key;
        }
    }

    pub fn get_key(&self) -> u64{
        self.key
    }
}