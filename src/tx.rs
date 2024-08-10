use std::{borrow::Borrow, cell::{RefCell, RefMut}, collections::HashMap, io::WriterPanicked, marker::PhantomData, sync::{Arc, Weak}};

use crate::{config::PAGE_SIZE, cursor::Cursor, db::{WeakDB, DB}, error::Error, node::{Node, NodeInner, WeakNode}, page::{Meta, OwnedPage, Page, PgId}, DEFAULT_FILL_PERCENT, MAX_KEY_SIZE, MAX_VALUE_SIZE};


use crate::error::Result;



use lock_api::{RawMutex, RawRwLock};





pub type TxId = u64;

#[derive(Clone)]
pub struct Tx(pub(crate) Arc<TxInner>);

#[derive(Default)]
pub struct WeakTx(pub(crate) Weak<TxInner>);


pub struct TxInner {
    pub(crate) writable: bool,
    pub weak_db: WeakDB,
    //pub(crate) root: RefCell<Bucket>,
    pub(crate) meta: RefCell<Meta>,
    pub(crate) pages: RefCell<HashMap<PgId, OwnedPage>>,
    pub(crate) nodes: RefCell<HashMap<PgId, Node>>,
    pub(crate) root_node: RefCell<Option<Node>>,
    pub(crate) fill_percent: f64,
}


impl Tx {
    pub fn id(&self) -> TxId {
        self.0.meta.borrow().txid
    }    

    pub fn rollback(&mut self) -> Result<()> {
        let db = self.0.weak_db.0.upgrade().unwrap();
        if self.0.writable {
            db.freelist.try_write().unwrap().rollback(self.id())?;
            let free_page = db.page(db.state.try_read().unwrap().borrow().meta().freelist);
            db.freelist.try_write().unwrap().reload(unsafe {&*free_page})?;
        }
        self.close()?;
        Ok(())

    }
    pub fn commit(&mut self) -> Result<()> {
        if !self.0.writable {
            return Ok(())
        }
        let db = self.db().unwrap();

        self.rebalance(PAGE_SIZE as usize)?;
        if let Err(e) = self.spill() {
            self.rollback()?;
            return Err(e);
        }
        //回收旧的freelist列表
        db.0.freelist
            .try_write()
            .unwrap()
            .free(self.0.meta.borrow().txid, unsafe {
                &*db.0.page(self.0.meta.borrow().freelist)
            });

        let size = db.0.freelist.try_read().unwrap().size();
        let mut p = match db.0.allocate(size /PAGE_SIZE as usize + 1) {
            Ok(_p) => _p,
            Err(e) => {
                self.rollback()?;
                return Err(e);
            }
        };

        let page = p.to_page_mut();
        db.0.freelist.try_write().unwrap().write(page);

        self.0.meta.borrow_mut().freelist = page.id;
        self.0.pages.borrow_mut().insert(page.id, p);
        //let roo = self.0.meta.borrow().root;
        // may be already update;
        //self.0.meta.borrow_mut().root = roo;
        let check_sum = self.0.meta.borrow().compute_checksum();
        self.0.meta.borrow_mut().checksum = check_sum;
        //write dirty page
        if let Err(e) = self.write() {
            self.rollback()?;
            return Err(e);
        }

        //write meta
        if let Err(e) = self.write_meta() {
            self.rollback()?;
            return Err(e);
        }

        self.close();
        Ok(())
    }
    pub fn close(&self) -> Result<()> {
        if self.0.writable {
            self.db().unwrap().0.remove_tx(self.clone()); 
            unsafe { self.0.weak_db.0.upgrade().unwrap().rw_lock.raw().unlock() };

        }else {
            unsafe {
                self.0.weak_db.0.upgrade().unwrap().state.raw().unlock_shared();
            }
        }
        Ok(())
    }



    pub fn new(writable: bool, weak_db: WeakDB, meta: Meta) -> Self {
        let tx = Arc::new(
            TxInner{
                writable,
                weak_db ,
                meta: RefCell::new(meta),
                pages: Default::default(),
                nodes: RefCell::new(HashMap::new()),
                root_node: Default::default(),
                fill_percent: DEFAULT_FILL_PERCENT,
                
            }
        );
        Tx(tx)
    }

    pub fn db(&self) -> Option<DB> {
        let ac = Weak::upgrade(&self.0.weak_db.0)?;
        Some(DB(
            ac
        ))
    }

    pub fn write(&self) -> Result<()> {
        let mut pages = self.0
            .pages
            .borrow_mut()
            .drain()
            .map(|(k, v)| (k, v))
            .collect::<Vec<(u64, OwnedPage)>>();
        pages.sort_by(|a, b| a.0.cmp(&b.0));

        for p in pages.iter() {
            let page = p.1.to_page();
            let offset = page.id * PAGE_SIZE as u64;
            self.db().unwrap().0.write_at(&p.1.value, offset)?;
        }
        self.db().unwrap().0.sync()?;
        Ok(())

    }
    pub fn write_meta(&self) -> Result<()> {
        let mut buf = vec![0u8; PAGE_SIZE];
        let id = {
            let p = Page::page_in_buffer_mut(&mut buf, 0);
            self.0.meta.borrow_mut().write(p);
            p.id =  self.0.meta.borrow().txid %2;
            p.id
        };

        self.db().unwrap().0.write_at(&buf, id * PAGE_SIZE as u64) ?;
        let ow = OwnedPage::from_vec(buf);
        self.db().unwrap().0.sync()?;
        Ok(())
    }
}


impl Tx {
    pub fn root_id(&self) -> PgId {
        self.0.meta.borrow().root
    }
    fn cursor(&mut self ) -> Cursor{
        Cursor::new(self.clone())
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
        if let Some(node) = self.0.nodes.borrow().get(&id) {
            return Ok(PageNode::Node(node.clone()));
        }
        let page = self.db().unwrap().0.page(id);
        Ok(PageNode::Page(page))
    }

    pub(crate) fn node(&self, pgid: PgId, parent: Option<WeakNode>) -> Node {
        if let Some(node) = self.0.nodes.borrow().get(&pgid) {
            return node.clone();
        }

        let mut n = if let Some(p) = parent {
            let n = NodeInner::new().parent(p.clone()).build();
            let parent_node =  p.upgrade().unwrap();
            parent_node.node_mut().children.push(n.clone());
            n
        } else {
            let n = NodeInner::new().build();
            self.0.root_node.replace(Some(n.clone()));
            n
        };

        let page = {
            let p = self.db().unwrap().0.page(pgid);
            unsafe { &*p }
        };
        n.read(page);
        self.0.nodes.borrow_mut().insert(pgid, n.clone());
        n
    }

    pub(crate) fn rebalance(&mut self, page_size: usize) -> Result<()> {
        let nodes = self.0.nodes.clone();
        for n in nodes.borrow_mut().values_mut() {
            n.rebalance(page_size,self)?;
        }
        Ok(())
    }


    pub(crate) fn spill(&mut self) -> Result<()> {
        let mut x = self.0.root_node.borrow_mut();
        if x.is_none() {
            return Ok(())
        }
        let n = x.as_ref().unwrap();
        let mut root = n.clone();
        let root_node = root.spill(self.clone())?;
        self.0.meta.borrow_mut().root = root_node.node().pgid;
        (x).replace(root_node);
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