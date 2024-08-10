use std::{borrow::Borrow, cell::{RefCell, RefMut}, collections::HashMap, io::WriterPanicked, marker::PhantomData, sync::{Arc, Weak}};

use crate::{bucket::Bucket, config::PAGE_SIZE, db::{WeakDB, DB}, page::{Meta, OwnedPage, Page, PgId}};


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
    pub(crate) root: RefCell<Bucket>,
    pub(crate) meta: RefCell<Meta>,
    pub(crate) pages: RefCell<HashMap<PgId, OwnedPage>>,
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

        self.0.root
            .borrow_mut()
            .rebalance(PAGE_SIZE as usize)?;
        if let Err(e) = self.0.root.borrow_mut().spill(self.clone()) {
            self.clone().rollback()?;
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
        let roo = self.bucket().unwrap().pg_id;
        self.0.meta.borrow_mut().root = roo;
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

    pub fn bucket<'a,'b: 'a>(&'b mut self) -> Result<RefMut<'a,Bucket>> {
        Ok(self.0.root.borrow_mut())
    }


    pub fn new(writable: bool, weak_db: WeakDB, meta: Meta) -> Self {
        let root_id = meta.root;
        let tx = Arc::new(
            TxInner{
                writable,
                weak_db ,
                root: RefCell::new(Bucket::default()),
                meta: RefCell::new(meta),
                pages: Default::default(),
            }
        );
        *tx.root.borrow_mut() = Bucket::new(root_id, WeakTx(Arc::downgrade(&tx)));
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