use std::{borrow::{Borrow, BorrowMut}, default, fs::{File, OpenOptions}, io::Write, ptr::null, sync::{atomic::AtomicU64, Arc, Weak}};

use parking_lot::Mutex;
use parking_lot::RwLock;
use crate::{config::{INITIAL_DB_SIZE, MAX_MMAP_SIZE, MAX_MMAP_STEP, PAGE_SIZE}, db, freelist::FreeList, page::{Meta, OwnedPage, Page, PageFlag, PgId, MAGIC, VERSION}, tx::{Tx, TxId, TxInner}};

use crate::error::{Error,Result};
use std::os::unix::fs::FileExt;
use lock_api::{RawMutex, RawRwLock};
pub struct DBInnerState {
    pub db_size: u64,
    pub meta0: *const Meta, 
    pub meta1: *const Meta,
    pub mmap: Option<memmap::Mmap>,
}

impl Default for DBInnerState {
    fn default() -> Self {
        Self{
            db_size: Default::default(),
            mmap: None,
            meta0: null(),
            meta1: null(),
        }
    }
}
impl DBInnerState {
    pub(crate) fn set_mmap(&mut self, file: &File, min_size: usize) -> Result<()> {
        let mut mmap_opts = memmap::MmapOptions::new();
        let mut size = file.metadata().map_err(|e| Error::DBOpenFail(e))?.len();

        size = size.max(self.mmap_size(min_size as u64)? as u64);
        let nmmap = unsafe {
            mmap_opts
                .offset(0)
                .len(size as usize)
                .map(file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        let meta0 = Page::page_in_buffer(&nmmap, 0).meta();
        let meta1 = Page::page_in_buffer(&nmmap, 1).meta();

        meta0.validate()?;
        meta1.validate()?;
        self.meta0 = meta0;
        self.meta1 = meta1;
        self.mmap.replace(nmmap);
        self.db_size = size as u64;
        Ok(())
    }

    pub(crate) fn meta(&self) -> Meta {
        unsafe {
            let mut meta_a = self.meta0;
            let mut meta_b = self.meta1;
            if (*self.meta1).txid > (*self.meta0).txid {
                meta_a = self.meta1;
                meta_b = self.meta0;
            }
            if (*meta_a).validate().is_ok() {
                return (*meta_a).clone();
            }
            if (*meta_b).validate().is_ok() {
                return (*meta_b).clone();
            }
            panic!(" invalid meta pages")
        }
    }
    fn mmap_size(&self, mut size: u64) -> Result<u64> {
        for i in 3..=30 {
            if size <= 1 << i {
                return Ok(1 << i);
            }
        }
        if size > MAX_MMAP_SIZE {
            return Err(Error::Unexpected("mmap too large".to_string()));
        }
        let remainder = size % MAX_MMAP_STEP;
        if remainder > 0 {
            size += MAX_MMAP_SIZE - remainder;
        };
        let page_size = PAGE_SIZE as u64;
        if (size % page_size) != 0 {
            size = ((size / page_size) + 1) * page_size;
        };
        if size > MAX_MMAP_SIZE {
            size = MAX_MMAP_SIZE
        };
        Ok(size)
    }

}
pub struct DBInner {
    pub file: RwLock<File>,
    pub freelist: RwLock<FreeList>,
    pub rw_tx: RwLock<Option<Tx>>,
    pub txs: RwLock<Vec<Tx>>,
    pub rw_lock: Mutex<()>,
    pub state: RwLock<DBInnerState>,

}

#[derive(Clone)]
pub struct DB(pub Arc<DBInner>);
pub struct WeakDB(pub Weak<DBInner>);


pub struct Options{
    pub initial_mmap_size: usize

}
impl DB {
    fn begin_rwtx(&self) -> Tx {
        unsafe {
            self.0.rw_lock.raw().lock();
        }
        let mut meta = self.0.state.try_read().unwrap().borrow().meta();
        meta.txid+=1;
        let mut tx = Tx::new(true, WeakDB(Arc::downgrade(&self.0)), meta);
        *(self.0.rw_tx.try_write().unwrap()) = Some(tx.clone());
        self.0.txs.write().push(tx.clone());
        let txs = self.0.txs.read();
        let minid = txs
            .iter()
            .map(|tx| tx.id())
            .min()
            .unwrap_or(0xFFFF_FFFF_FFFF_FFFF);
        if minid > 0 {
            self.0.freelist.try_write().unwrap().release(minid - 1);
        }
        drop(txs);
        tx

    }

    fn begin_tx(&self) -> Tx {
        unsafe {
            self.0.state.raw().lock_shared();
        }
        let mut meta = self.0.state.try_read().unwrap().borrow().meta();
        let mut tx = Tx::new(false, WeakDB(Arc::downgrade(&self.0)), meta);
        self.0.txs.try_write().unwrap().push(tx.clone());
        tx
    }

    pub fn open(path: &str,  opt: Options) -> Self{
        DBInner::open(path, opt).unwrap()
    }

}

impl DBInner {
    pub fn new(file: File) -> Self {
        Self{
            file: RwLock::new(file),
            freelist: RwLock::new(FreeList::default()),
            rw_tx:RwLock::new(None), 
            txs: RwLock::new(Vec::default()),
            rw_lock: Mutex::new(()),
            state: RwLock::new(Default::default()),
        } 
    }

    pub fn open(path: &str, opt: Options) -> Result<DB> {

        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map_err(|e| Error::DBOpenFail(e))?; 
        f.set_len(0);

        let size = f.metadata().map_err(|e| Error::DBOpenFail(e))?.len();
        let mut db = Self::new(f);
        if size == 0 {
            db.init()?;
        }
        db.state.try_write().unwrap().set_mmap(&db.file.try_read().unwrap(),0)?;
        let meta = db.state.try_read().unwrap().meta();
        {
            let t = db.state.try_read().unwrap();
            let tt = t.mmap.as_ref().unwrap().as_ptr();
            unsafe {
                let buf = std::slice::from_raw_parts(tt, t.db_size as usize);
                db.freelist.try_write().unwrap().read(
                    Page::page_in_buffer(buf, meta.freelist)
                );
            }
        } 
        Ok(DB(Arc::new(db)))
    }

    pub fn init(&mut self) -> Result<()> {
        let mut buf: Vec<u8> = vec![0; 4 * PAGE_SIZE];
        for i in 0..2 {
            let p = Page::page_in_buffer_mut(&mut buf, i);
            p.id = i as PgId;
            p.flags = PageFlag::MetaPage;

            let m = p.meta_mut();
            m.magic = MAGIC;
            m.version = VERSION;
            m.freelist = 2;
            m.root = 3;
            m.pgid = 4;
            m.txid = i as TxId;
            m.checksum = m.compute_checksum();
        }

        let mut p = Page::page_in_buffer_mut(&mut buf, 2);
        p.id = 2;
        p.flags = PageFlag::FreeListPage;
        p.count = 0;

        p = Page::page_in_buffer_mut(&mut buf, 3);
        p.id = 3;
        p.flags = PageFlag::LeafPage;
        p.count = 0;

        self.write_at(&buf, 0)?;
        self.sync()?;


        Ok(())

    }
    pub(crate) fn write_at(&self, buf: &[u8], pos: u64) -> Result<()> {
        self.file
            .try_write()
            .unwrap()
            .write_at(buf, pos)
            .map_err(|_e| ("can't write to file", _e))?;
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.file
            .try_write()
            .unwrap()
            .flush()
            .map_err(|_e| ("can't flush file", _e))?;
        Ok(())
    }

    pub fn page(&self, id: PgId) -> *const Page {
        let r = self.state.try_read().unwrap();
        let s = r.mmap.as_deref().unwrap();
        let ptr = unsafe { s.as_ptr().add(id as usize * PAGE_SIZE as usize)} as*const Page;
        ptr
    }

    pub(crate) fn remove_tx(&self, tx: Tx) {
        let mut txs = self.txs.write();
        let index = txs.iter().position(|t| Arc::ptr_eq(&tx.0, &t.0)).unwrap();
        txs.remove(index);
    }

    pub(crate) fn allocate(&self, count: usize) -> Result<OwnedPage> {
        let mut page = 
            OwnedPage::from_vec(vec![0u8; PAGE_SIZE * count]);

        let p = page.to_page_mut();
        p.overflow = (count - 1) as u32;

        p.id = self.freelist.try_write().unwrap().allocate(count);
        if p.id != 0 {
            return Ok(page);
        }

        p.id = (*(self.rw_tx.try_write().unwrap().as_ref().unwrap().0))
            .meta
            .borrow()
            .pgid;

        let minsz = (((p.id + count as PgId + 1) as usize) * PAGE_SIZE) as u64;
        if minsz >= self.state.try_read().unwrap().db_size {
            self.state
                .try_write()
                .unwrap()
                .set_mmap(&self.file.try_read().unwrap(), minsz as usize)?;
        }

        
        self.rw_tx.try_write().unwrap().as_ref().unwrap().0.meta.borrow_mut().pgid += count as PgId;

        Ok(page)
    }

}


const DEFAULT_OPTIONS: Options = Options {
    initial_mmap_size: INITIAL_DB_SIZE
};
#[cfg(test)]
mod tests {
    use crate::config::INITIAL_DB_SIZE;

    use super::*;
    use core::time;
    use std::str;
    use std::thread;
    use std::thread::sleep;
    use std::thread::JoinHandle;
    use std::thread::Thread;
    use std::time::Duration;

    #[test]
    fn test_multi_thread() {
        let db = DB::open("./test1.db", Options { initial_mmap_size: INITIAL_DB_SIZE });
        let mut v = vec![];
        let s = std::time::Instant::now(); 
        for i in 0..3000{
            let mut tx = db.begin_rwtx();
            tx.put(i.to_string().as_bytes(), i.to_string().as_bytes());
            assert_eq!(tx.get(i.to_string().as_bytes()).unwrap(),i.to_string().as_bytes());
            tx.commit();
        }
        println!("{:?}", s.elapsed());
        for i in 27..=27{
            let k = i % 100;
            let mut tx = db.begin_tx();
            v.push(thread::spawn(move || {
                //dbg!(k.to_string().as_bytes());
                assert_eq!(tx.get(k.to_string().as_bytes()),Some(k.to_string().as_bytes()));
//                assert_eq!(tx.get(k.to_string().as_bytes()),Some(k.to_string().as_bytes()));
                //assert_eq!(tx.get(k.to_string().as_bytes()),Some(k.to_string().as_bytes()));
                //assert_eq!(tx.get(k.to_string().as_bytes()),Some(k.to_string().as_bytes()));
                //assert_eq!(tx.get(k.to_string().as_bytes()),Some(k.to_string().as_bytes()));
                tx.commit();
                //println!("finish r {}",i);
            }));
        }
        for i in v {
            i.join();
        }


    }
    #[test]
    fn test_db_mmap() {
        let db = DB::open("./test1.db", Options { initial_mmap_size: INITIAL_DB_SIZE });
        let mut tx = unsafe { (&*(db.0.state.try_read().unwrap().meta0)).txid };
        let mut buf = vec![0; 4096];
        let page =
            Page::page_in_buffer_mut(&mut buf, 0);
        let meta = page.meta_mut();
        meta.txid = 2;
        db.0.write_at(&buf, 0).unwrap();
        db.0.sync().unwrap();
        let mut tx = db.begin_rwtx();
        tx.commit();
        assert_eq!(tx.id(),2);
    }


    #[test]
    fn test_tx_delete() {
        let mut db = DBInner::open("./test2.db", DEFAULT_OPTIONS).unwrap();
        let mut tx1 = db.begin_rwtx();
        tx1.put(b"001", b"123");
        tx1.put(b"005", b"ccc");
        tx1.commit();
        let mut tx2 = db.begin_rwtx();
        tx2.put(b"002", b"bbb");
        tx2.put(b"003", b"ccc");
        tx2.put(b"004", b"ddd");
        tx2.commit();

        let mut tx3 = db.begin_rwtx();
        tx3.delete(b"001");
        assert_eq!(tx3.get(b"001"),None);
        tx3.commit();


        let mut tx4 = db.begin_tx();
        assert_eq!(tx4.get(b"002").unwrap(),b"bbb");
        assert_eq!(tx4.get(b"004").unwrap(),b"ddd");
        assert_eq!(tx4.get(b"001"),None);
        tx4.commit();
    }
    #[test]
    fn test_tx_put_get() {
        let mut db = DBInner::open("./test2.db", DEFAULT_OPTIONS).unwrap();
        dbg!(db.0.state.try_read().unwrap().meta().root);
        let mut tx1 = db.begin_rwtx();
        tx1.put(b"001", b"aaa");
        tx1.put(b"005", b"ccc");
        tx1.commit().unwrap();

        dbg!(db.0.state.try_read().unwrap().meta().root);
        dbg!(db.0.state.try_read().unwrap().meta().txid);
        let mut tx4 = db.begin_tx();
        assert_eq!(tx4.get(b"001").unwrap(),b"aaa");
        assert_eq!(tx4.get(b"008"),None);
        tx4.commit();
    }
    //#[test]
    //fn test_db_print() {
        //let mut db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        //db.print();
    //}
}
