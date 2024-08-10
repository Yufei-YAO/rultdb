

use std::{marker::PhantomData, mem::offset_of};
use crate::config::PAGE_SIZE;
use crate::error::Result;
use crate::error::Error;
use bitflags::bitflags;
use bitflags::Flags;

use std::mem::size_of;
use crate::tx::TxId;

bitflags! {
    pub struct PageFlag: u16 {
        const BranchPage = 0x01;
        const LeafPage = 0x02;
        const MetaPage = 0x04;
        const FreeListPage = 0x08;
    }
}

pub const PAGE_HEADER_SIZE: usize = offset_of!(Page,ptr);
pub const MIN_KEY_PERPAGE: usize = 2;


pub const META_SIZE:usize = size_of::<Meta>();
pub const LEAF_ELEMENT_SIZE:usize = size_of::<LeafPageElement>();
pub const BRANCH_ELEMENT_SIZE:usize = size_of::<BranchPageElement>();

pub type PgId = u64;

pub const MAGIC:u32 = 0x4499;
pub const VERSION:u32 = 0x01;


pub struct Page{
    pub id: PgId,
    pub flags: PageFlag,
    pub count: u16,
    pub overflow: u32,
    pub ptr: PhantomData<u8>,
}

pub struct OwnedPage{
    pub value: Vec<u8>
}

impl OwnedPage {
    pub fn from_vec(value: Vec<u8>) -> Self {
        Self {
            value : value
        }
    }

    pub fn to_page_mut(&mut self) -> &mut Page {
        unsafe {
            &mut *(self.value.as_mut_ptr() as *mut Page)
        }

    }
    pub fn to_page(&self) ->&Page {
        unsafe {
            &*(self.value.as_ptr() as *const Page)
        }
    }
}

pub struct BranchPageElement {
    pub pos: u32,
    pub ksize: u32,
    pub value: PgId,
}

pub struct LeafPageElement {
    pub pos: u32,
    pub ksize: u32,
    pub vsize: u32,
}


impl BranchPageElement {
    pub fn key(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts((self as *const BranchPageElement as *const u8).add(self.pos as usize), self.ksize as usize)
        }
    } 
}

impl LeafPageElement {
    pub fn key(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts((self as *const LeafPageElement as *const u8).add(self.pos as usize), self.ksize as usize)
        }
    } 
    pub fn value(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts((self as *const LeafPageElement as *const u8).add((self.pos + self.ksize ) as usize), self.vsize as usize)
        }
    } 
}

#[derive(Clone)]
pub struct Meta{
    pub magic: u32,
    pub version: u32,
    pub flags: u32,
    pub root: PgId,
    pub freelist: PgId,
    pub pgid: PgId,
    pub txid: TxId,
    pub checksum: u32,
}

impl Meta {
    pub fn compute_checksum(&self) -> u32{
        let data = unsafe {
            std::slice::from_raw_parts(self as *const Meta as *const u8, offset_of!(Meta,checksum) )
        };
        crc32fast::hash(data)
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::ErrInvalid);
        }else if self.version != VERSION {
            return Err(Error::ErrVersionMismatch);
        }else if self.checksum != self.compute_checksum() {
            return Err(Error::ErrChecksum);
        }
        Ok(())
    }
    pub fn write(&mut self, p: &mut Page) {
        self.checksum = self.compute_checksum();
        p.flags = PageFlag::MetaPage;
        p.count = 0;
        p.overflow = 0;
        *p.meta_mut() = self.clone();
    }


}

impl Page {
    pub fn from_mut_buf(buf: &mut [u8]) -> &mut Page {
        unsafe{
            &mut *(buf.as_mut_ptr() as *mut Page )
        }
    }
    pub fn from_buf(buf: & [u8]) -> &Page {
        unsafe{
            &*(buf.as_ptr() as *const Page )
        }
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        //unsafe {
            //&mut*(self.data_mut_ptr() as *mut Meta)    
        //}
        self.element_mut::<Meta>()
    }
    pub fn meta(& self) -> &Meta {
        self.element::<Meta>()

    }
    pub(crate) fn data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }
    pub(crate) fn data_mut_ptr(&self) -> *mut u8 {
        &self.ptr as *const PhantomData<u8> as *mut u8
    }
    fn elements<T>(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.data_ptr() as *const T, self.count as usize) }
    }


    fn elements_mut<T>(&mut self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(self.data_mut_ptr() as *mut T, self.count as usize)
        }
    }
    fn elements_mut_with_size<T>(&mut self, size: usize) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(self.data_mut_ptr() as *mut T, size)
        }
    }

    fn element<T>(&self) -> &T {
        unsafe { &*(self.data_ptr() as *const T) }
    }

    fn element_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.data_mut_ptr() as *mut T) }
    }

    fn leaf_page_elements_mut(&mut self) -> &mut [LeafPageElement] {
        self.elements_mut::<LeafPageElement>()
    }

    fn branch_page_elements_mut(&mut self) -> &mut [BranchPageElement] {
        self.elements_mut::<BranchPageElement>()
    }

    pub(crate) fn branch_page_elements(&self) -> &[BranchPageElement] {
        self.elements::<BranchPageElement>()
    }

    pub(crate) fn leaf_page_elements(&self) -> &[LeafPageElement] {
        self.elements::<LeafPageElement>()
    }

    pub(crate) fn freelist(&self) -> &[PgId] {
        self.elements::<PgId>()
    }

    pub(crate) fn freelist_mut(&mut self) -> &mut [PgId] {
        self.elements_mut::<PgId>()
    }
    pub(crate) fn freelist_mut_with_size(&mut self,size : usize) -> &mut [PgId] {
        self.elements_mut_with_size::<PgId>(size)
    }

    pub(crate) fn leaf_page_element(&self, index: usize) -> &LeafPageElement {
        self.leaf_page_elements().get(index).unwrap()
    }

    pub(crate) fn branch_page_element(&self, index: usize) -> &BranchPageElement {
        self.branch_page_elements().get(index).unwrap()
    }

    pub(crate) fn leaf_page_element_mut(&mut self, index: usize) -> &mut LeafPageElement {
        self.leaf_page_elements_mut().get_mut(index).unwrap()
    }

    pub(crate) fn branch_page_element_mut(&mut self, index: usize) -> &mut BranchPageElement {
        self.branch_page_elements_mut().get_mut(index).unwrap()
    }
    pub(crate) fn page_in_buffer_mut(buf: & mut [u8], id: PgId) -> & mut Page {
        Page::from_mut_buf(&mut buf[(id as usize * PAGE_SIZE)..])
    }

    pub(crate) fn page_in_buffer( buf: & [u8], id: PgId) -> & Page {
        Page::from_buf(&buf[(id as usize * PAGE_SIZE) as usize..])
    }
}