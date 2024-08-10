use std::{cell::RefCell, collections::HashMap };

use crate::{cursor::Cursor, db, error::{Error, Result}, node::{Node, NodeInner, WeakNode}, page::{Page, PgId}, tx::{Tx, WeakTx}, DEFAULT_FILL_PERCENT, MAX_KEY_SIZE, MAX_VALUE_SIZE};



#[derive(Default)]
pub struct Bucket {
    pub(crate) pg_id: PgId,
    pub(crate) nodes: RefCell<HashMap<PgId, Node>>,
    pub(crate) weak_tx: WeakTx,
    root_node: Option<Node>,
    pub(crate) fill_percent: f64,
}

impl Bucket {
    pub fn new(pg_id: PgId, tx: WeakTx) -> Self{
        Self{
            pg_id,
            nodes : RefCell::new(HashMap::new()),
            weak_tx: tx,
            root_node: None,
            fill_percent: DEFAULT_FILL_PERCENT
        }
    } 

    fn cursor(&mut self ) -> Cursor{
        Cursor::new(self)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() == 0 {
            return Err(Error::ErrKeyRequired);
        } else if key.len() > MAX_KEY_SIZE {
            return Err(Error::ErrKeyTooLarge);
        } else if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ErrValueTooLarge);
        }

        let mut c = self.cursor();
        let item = c.seek(key)?;

        c.node()?.put(key, key, value, 0);
        Ok(())
    }


    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        let mut c = self.cursor();
        let item  = c.seek(key).unwrap();
        if item.0.is_none() {
            return None;
        }
        if item.0.unwrap() ==  key {
            return item.1
        }else {
            return None;
        }

    }
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let mut c = self.cursor();
        c.seek(key)?;
        c.node()?.del(key);
        Ok(())
    }
    pub(crate) fn page_node(&self, id: PgId) -> Result<PageNode> {
        if let Some(node) = self.nodes.borrow().get(&id) {
            return Ok(PageNode::Node(node.clone()));
        }
        let page = self.tx().unwrap().db().unwrap().0.page(id);
        Ok(PageNode::Page(page))
    }
    pub(crate) fn tx(&self) -> Option<Tx> {
        Some(Tx(self.weak_tx.0.upgrade()?))
    }

    pub(crate) fn node(&mut self, pgid: PgId, parent: Option<WeakNode>) -> Node {
        if let Some(node) = self.nodes.borrow().get(&pgid) {
            return node.clone();
        }

        let mut n = if let Some(p) = parent {
            let n = NodeInner::new().parent(p.clone()).build();
            let parent_node =  p.upgrade().unwrap();
            parent_node.node_mut().children.push(n.clone());
            n
        } else {
            let n = NodeInner::new().build();
            self.root_node.replace(n.clone());
            n
        };

        let page = {
            let p = self.tx().unwrap().db().unwrap().0.page(pgid);
            unsafe { &*p }
        };
        n.read(page);
        self.nodes.borrow_mut().insert(pgid, n.clone());
        n
    }

    pub(crate) fn rebalance(&mut self, page_size: usize) -> Result<()> {
        let nodes = self.nodes.clone();
        for n in nodes.borrow_mut().values_mut() {
            n.rebalance(page_size, self)?;
        }
        Ok(())
    }


    pub(crate) fn spill(&mut self, atx: Tx) -> Result<()> {
        if let Some(n) = &self.root_node {
            let mut root = n.clone();
            let root_node = root.spill(atx, &self)?;
            self.pg_id = root_node.node().pgid;
            self.root_node.replace(root_node);
        }
        Ok(())
    }
}


#[derive(Clone)]
pub(crate) enum PageNode {
    Page(*const Page),
    Node(Node),
}

impl From<Node> for PageNode {
    fn from(n: Node) -> Self {
        PageNode::Node(n)
    }
}