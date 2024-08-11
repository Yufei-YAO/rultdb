use std::{collections::HashMap, default, mem::size_of};

use crate::{page::{Page, PageFlag, PgId, PAGE_HEADER_SIZE}, tx::TxId};
use crate::error::Result;



#[derive(Default,Debug)]
pub struct FreeList {
    pub(crate) ids: Vec<PgId>,
    pub(crate) pending: HashMap<TxId, Vec<PgId>>,
}


impl FreeList {
    pub fn size(&self) -> usize {
        let mut count = self.count();
        if count > 0xFFFF {
            count += 1;
        }
        return PAGE_HEADER_SIZE + size_of::<PgId>() * count;
    }

    fn count(&self) -> usize {
        self.pending_count() + self.free_count()
    }

    fn free_count(&self) -> usize {
        self.ids.len()
    }

    fn pending_count(&self) -> usize {
        self.pending.iter().map(|x| x.1.len()).sum()
    }


    pub fn rollback(&mut self, txid: TxId) -> Result<()> {
        self.pending.remove(&txid);
        Ok(())
    }

    pub fn free(&mut self, txid: TxId, p: &Page) {
        //if p.id == 4 {
            //dbg!(&*self);
        //}
        assert!(p.id > 1, "cannot free page {}",p.id);
        let ids = self.pending.entry(txid).or_insert(Vec::new());
        for id in p.id..=p.id + p.overflow as PgId {
            ids.push(id);
        }
    }

    pub fn allocate(&mut self, n: usize) -> PgId {
        //dbg!(&*self);
        if self.ids.len() == 0 {
            return 0;
        }
//        let mut initial: PgId = 0;
        //let mut previd: PgId = 0;
        //let mut end_alloc = 0;
        //for (i, id) in self.ids.iter().enumerate() {
            //assert!(*id > 1, "invalid free page {}",id);

            //if previd == 0 || id-previd != 1 {
                //initial = *id;
            //}
            //if (*id - initial) +1 == n as PgId {
                //end_alloc = i;
                //break;
            //}
        //}
        //if end_alloc == 0 {
            ////return 0;
        //}
        //dbg!(initial);
        let mut initial: PgId = 0;
        let mut previd: PgId = 0;
        let item = self.ids.iter().enumerate().position(|(_i, _id)| {
            let id = *_id;
            assert!(id > 1, "invalid free page {}",id);

            if previd == 0 || id - previd != 1 {
              initial = id;
            }
            if (id - initial) + 1 == n as PgId {
              return true;
            }
            previd = id;
            false
        });
        return match item {
            Some(index) => {
              //dbg!(initial);
              self.ids.drain(index - (n - 1)..index + 1);
              initial
            }
            None => 0,
        };
    }
    
    pub(crate) fn read(&mut self, p: &Page) {
        let mut idx: usize = 0;
        let mut count = p.count as usize;
        if count == 0xFFFF {
            idx = 1;
            count = *p.freelist().first().unwrap() as usize;
        }
        if count == 0 {
            self.ids.clear();
        } else {
            let ids = p.freelist();
            self.ids = ids[idx..count].to_vec();
            self.ids.sort_unstable();
        }

    }



    pub(crate) fn write(&self, p: &mut Page) {
        p.flags |= PageFlag::FreeListPage;

        let count = self.count();
        if count == 0 {
            p.count = count as u16;
        } else if count < 0xFFFF {
            p.count = count as u16;
            let m = p.freelist_mut();
            self.copy_all(m);
            m.sort_unstable();
        } else {
            p.count = 0xFFFF;
            let m = p.freelist_mut_with_size(count);
            m[0] = count as u64;
            self.copy_all(&mut m[1..]);
            m[1..].sort_unstable();
        }
    }

    pub(crate) fn copy_all(&self, mut dst: &mut [PgId]) {
        let mut m: Vec<PgId> = Vec::with_capacity(self.pending_count());
        for list in self.pending.values() {
            dst[..list.len()].copy_from_slice(list);
            dst = &mut dst[list.len()..];
        }
        dst[..self.ids.len()].copy_from_slice(self.ids.as_slice())
    }

    pub(crate) fn reload(&mut self, p: &Page) -> Result<()> {
        self.read(p);
        let mut pcache: HashMap<PgId, bool> = HashMap::new();
        for pending_ids in self.pending.values() {
            for pending_id in pending_ids.iter() {
                pcache.insert(*pending_id, true);
            }
        }
        let mut a: Vec<PgId> = Vec::new();
        for id in self.ids.iter() {
            if !pcache.contains_key(id) {
                a.push(*id);
            }
        }
        self.ids = a;
        Ok(())
    }

    pub(crate) fn release(&mut self, txid: TxId) {
        let mut m: Vec<PgId> = Vec::new();
        let mut remove_txid: Vec<TxId> = Vec::new();
        self.pending.iter().for_each(|(tid,ids)| {
            if *tid < txid {
                m.extend_from_slice(ids);
                remove_txid.push(*tid);
            }
        });

        for txid in remove_txid {
            self.pending.remove(&txid);
        }
        let mut v = Vec::with_capacity(self.ids.len() + m.len());
        v.extend(self.ids.iter());
        v.extend(m.iter());
        v.sort_unstable();
        self.ids = v;

    }


}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    #[test]
    fn test_freelist_allocate() {
        let ids: Vec<PgId> = vec![2, 3];
        let mut freelist = FreeList {
            ids: ids,
            pending: HashMap::new(),
    
        };
        let pgid = freelist.allocate(1);
    }

    #[test]
    fn test_count() {
        let ids: Vec<PgId> = vec![2, 3, 6, 7, 5];

        let id1: Vec<PgId> = vec![2, 3, 6, 7];
        let id2: Vec<PgId> = vec![2, 3, 6, 7];

        let mut map: HashMap<TxId, Vec<PgId>> = HashMap::new();

        map.insert(1, id1);
        map.insert(2, id2);

        let mut freelist = FreeList {
            ids: ids,
            pending: map,
   
        };
    }

    #[test]
    fn test_copy_all() {
        let ids: Vec<PgId> = vec![1, 2, 5, 6, 7];

        let id1: Vec<PgId> = vec![3, 8];
        let id2: Vec<PgId> = vec![9, 10];

        let mut map: HashMap<TxId, Vec<PgId>> = HashMap::new();

        map.insert(1, id1);
        map.insert(2, id2);

        let mut freelist = FreeList {
            ids: ids,
            pending: map,
 
        };
        let mut dst: Vec<PgId> = vec![0; 10];
        freelist.copy_all(&mut dst);
        dst.sort_unstable();
    }
}
