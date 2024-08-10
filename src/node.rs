use std::{borrow::{Borrow, BorrowMut}, cell::{Ref, RefCell, RefMut}, sync::{Arc, Weak}};

use crate::{bucket::Bucket, config::PAGE_SIZE, db, page::{BranchPageElement, LeafPageElement, Page, PageFlag, PgId, BRANCH_ELEMENT_SIZE, LEAF_ELEMENT_SIZE, MIN_KEY_PERPAGE, PAGE_HEADER_SIZE}, tx::Tx, MAX_FILL_PERCENT, MIN_FILL_PERCENT};

use crate::error::Result;
#[derive(Clone)]
pub(crate) struct Node(pub(crate) Arc<RefCell<NodeInner>>);
#[derive(Clone)]
pub(crate) struct WeakNode(pub(crate) Weak<RefCell<NodeInner>>);

impl WeakNode {
    pub fn upgrade(&self) -> Option<Node> {
        Some(Node(
            self.0.upgrade()?
        ))
    } 
}
#[derive(Clone)]
pub(crate) struct NodeInner {
    pub(crate) parent: Option<WeakNode>,
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    unbalanced: bool,
    spilled: bool,
    pub(crate) pgid: PgId,
    pub(crate) children: Vec<Node>,
    key: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct INode {
    pub(crate) pgid: PgId,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}


impl NodeInner {
    pub(crate) fn new() -> NodeInner {
        Self {
            is_leaf: false,
            inodes: Vec::new(),
            parent: None,
            unbalanced: false,
            spilled: false,
            pgid: 0,
            children: Vec::new(),
            key: None,
        }
    }

    pub fn leaf(mut self, is_leaf: bool) -> NodeInner {
        self.is_leaf = is_leaf;
        self
    }

    pub fn parent(mut self, parent: WeakNode) -> NodeInner {
        self.parent = Some(parent);
        self
    }

    pub(crate) fn build(self) -> Node {
        Node(Arc::new(RefCell::new(self)))
    }
}


impl Node {
    pub fn node(&self) -> Ref<NodeInner> {
        (*(self.0)).borrow()

    } 
    pub(crate) fn node_mut(&self) -> RefMut<NodeInner> {
        (*(self.0)).borrow_mut()
    }
    fn min_keys(&self) -> usize {
        if self.node().is_leaf {
            1
        } else {
            2
        }
    }
    pub(crate) fn size(&self) -> usize {
        let mut sz = PAGE_HEADER_SIZE;
        let elsz = self.page_element_size();
        let a = self.node();
        for i in 0..a.inodes.len() {
            let item = a.inodes.get(i).unwrap();
            sz += elsz + item.key.len() + item.value.len();
        }
        sz
    }
    fn page_element_size(&self) -> usize {
        if self.node().is_leaf {
            return LEAF_ELEMENT_SIZE;
        }
        BRANCH_ELEMENT_SIZE 
    }

    pub(crate) fn child_at(
        &self,
        bucket: &mut Bucket,
        index: usize,
        parent: Option<WeakNode>,
    ) -> Node {
        if self.node().is_leaf {
            panic!("invalid childAt{} on a leaf node", index);
        }
        let pgid = self.node().inodes[index].pgid;
        bucket.node(pgid, parent)
    }


    pub(crate) fn read(&mut self, p: &Page) {
        let mut node_mut = self.node_mut();
        node_mut.pgid = p.id;
        node_mut.is_leaf = p.flags.contains(PageFlag::LeafPage);
        let count = p.count as usize;
        node_mut.inodes = Vec::with_capacity(count);
        for i in 0..count {
            let mut inode = INode::default();
            if node_mut.is_leaf {
                let elem = p.leaf_page_element(i);
                inode.key = elem.key().to_vec();
                inode.value = elem.value().to_vec();
            } else {
                let elem = p.branch_page_element(i);
                inode.pgid = elem.value;
                inode.key = elem.key().to_vec();
            }
            assert!(inode.key.len() > 0, "read: zero-length inode key");
            node_mut.inodes.push(inode);
        }
        if node_mut.inodes.len() > 0 {
            let key = { node_mut.inodes.first().unwrap().key.clone() };
            node_mut.key = Some(key);
        } else {
            node_mut.key = None
        }
    }

    pub(crate) fn del(&mut self, key: &[u8]) {
        let (exact, index) = {
            match self
                .node()
                .inodes
                .binary_search_by(|inode| inode.key.as_slice().cmp(key))
            {
                Ok(v) => (true, v),
                Err(e) => (false, e),
            }
        };
        dbg!(exact);
        if !exact {
            return;
        }
        self.node_mut().inodes.remove(index);
        self.node_mut().unbalanced = true;

    }


    pub(crate) fn put(&self, old_key: &[u8], new_key: &[u8], value: &[u8], pgid: PgId) {
        if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }
        let (exact, index) = {
            match self
                .node()
                .inodes
                .binary_search_by(|inode| inode.key.as_slice().cmp(old_key))
            {
                Ok(v) => (true, v),
                Err(e) => (false, e),
            }
        };
        {
            let mut n1 = self.node_mut();
            if !exact {
                n1.inodes.insert(index, INode::default());
            }
            let inode = n1.inodes.get_mut(index).unwrap();
            inode.key = new_key.to_vec();
            inode.value = value.to_vec();
            inode.pgid = pgid;
            assert!(inode.key.len() > 0, "put: zero-length inode key")
        }
    }
    pub(crate) fn write(&self, p: &mut Page) {
        if self.node().is_leaf {
            p.flags = PageFlag::LeafPage;
        } else {
            p.flags = PageFlag::BranchPage;
        }
        assert!(self.node().inodes.len() < 0xFFFF);
        p.count = self.node().inodes.len() as u16;
        if p.count == 0 {
            return;
        }

        let mut buf_ptr = unsafe {
            p.data_mut_ptr()
                .add(self.page_element_size() * self.node().inodes.len())
        };

        for (i, item) in self.node().inodes.iter().enumerate() {
            assert!(item.key.len() > 0, "write: zero-length inode key");
            if self.node().is_leaf {
                let elem = p.leaf_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem as *const LeafPageElement as usize) } as u32;
                elem.ksize = item.key.len() as u32;
                elem.vsize = item.value.len() as u32;
            } else {
                let elem = p.branch_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem as *const BranchPageElement as usize) } as u32;
                elem.ksize = item.key.len() as u32;
                elem.value = item.pgid;
            }
            let (klen, vlen) = (item.key.len(), item.value.len());
            unsafe {
                std::ptr::copy_nonoverlapping(item.key.as_ptr(), buf_ptr, klen);
                buf_ptr = buf_ptr.add(klen);
                std::ptr::copy_nonoverlapping(item.value.as_ptr(), buf_ptr, vlen);
                buf_ptr = buf_ptr.add(vlen);
            }
        }
    }


    fn child_index(&self, key: &[u8]) -> usize {
        match self
            .node()
            .inodes
            .binary_search_by(|inode| inode.key.as_slice().cmp(key))
        {
            Ok(v) => v,
            Err(e) => e,
        }
    }

    fn parent(&self) -> Option<Node> {
        match &self.node().parent {
            None => None,
            Some(p) => p.upgrade(),
        }
    }

    fn num_children(&self) -> usize {
        self.node().inodes.len()
    }

    fn next_sibling(&self, bucket: &mut Bucket) -> Option<Node> {
        match self.parent() {
            None => None,
            Some(mut p) => {
                let index = p.child_index(self.node().key.as_ref().unwrap());
                if index as isize > self.num_children() as isize - 1 {
                    return None;
                }
                Some(p.child_at(bucket, index + 1, Some(WeakNode(Arc::downgrade(&p.0)))))
            }
        }
    }

    fn prev_sibling(&self,bucket: &mut Bucket) -> Option<Node> {
        match self.parent(){
            None => None,
            Some(p) => {
                let index = p.child_index(self.node().key.as_ref().unwrap());
                if index == 0 {
                    return None;
                }
                Some(p.child_at(bucket, index - 1,Some(WeakNode(Arc::downgrade(&p.0)))))
            }
        }
    }

    pub(crate) fn free(&mut self, bucket: &Bucket) {
        if self.node().pgid != 0 {
            let tx = bucket.tx().unwrap();
            let db = tx.db().unwrap();
            db.0.freelist
                .try_write()
                .unwrap()
                .free(bucket.tx().unwrap().0.meta.borrow().txid, unsafe {
                    &*db.0.page(self.node().pgid)
                });
            self.node_mut().pgid = 0;
        }
    }


    fn remove_child(&mut self, target: Node) {
        let index = self
            .node()
            .children
            .iter()
            .position(|c| Arc::ptr_eq(&target.0, &c.0));
        if let Some(i) = index {
            self.node_mut().children.remove(i);
        }
    }

    pub(crate) fn rebalance(&mut self, page_size: usize, b: *const Bucket) -> Result<()> {
        let bucket = unsafe { &mut *(b as *mut Bucket) };
        if !self.node_mut().unbalanced {
            return Ok(());
        }
        self.node_mut().unbalanced = false;
        let threshold = page_size / 4;
        if self.size() > threshold && self.node().inodes.len() > self.min_keys() {
            return Ok(());
        }
        //当前节点是根节点，特殊处理
        if self.parent().is_none() {
            //当前节点是branch节点并且只有一个一个inode, 分裂当前节点
            if !self.node().is_leaf && self.node().inodes.len() == 1 {
                // 将root节点的叶子节点上移
                // 创建一个新的子节点，以当前节点作为root节点
                let pgid = self.node().inodes[0].pgid;
                let mut child = bucket.node(pgid, Some(WeakNode(Arc::downgrade(&self.0))));

                let mut node_mut = self.node_mut();
                node_mut.is_leaf = child.node().is_leaf;
                node_mut.inodes = child.node_mut().inodes.drain(..).collect();
                node_mut.children = child.node_mut().children.drain(..).collect();
                //删除老得叶子节点
                child.node_mut().parent = None;
                bucket.nodes.borrow_mut().remove(&child.node().pgid);
                child.free(bucket);
            }
            return Ok(());
        }
        let mut p = self.parent().unwrap();
        //如果当前的节点没有存储key ,移除当前的节点
        if self.num_children() == 0 {
            //如果当前的节点没有叶子节点，并行size<threshold
            //移除当前这个节点
            if let Some(k) = &self.node().key {
                p.del(k);
            }
            p.remove_child(self.clone());
            let pgid = self.node().pgid;
            bucket.nodes.borrow_mut().remove(&pgid);
            self.free(bucket); //释放当前节点对应的page
            p.rebalance(page_size, bucket)?;
            return Ok(());
        }
        //下面的情况是当前节点有数据
        let use_next_sibing = p.child_index(self.node().key.as_ref().unwrap()) == 0; //找到需要rebalance的节点的位置
        let mut target = if use_next_sibing {
            //当前节点是最左边的节点
            self.next_sibling(bucket).unwrap()
        } else {
            //左边的兄弟节点
            self.prev_sibling(bucket).unwrap()
        };
        // 如果当前节点和target节点都太小了，则合并他们
        if use_next_sibing {
            for inode in target.node().inodes.iter() {
                //如果目标节点是当前节点的右边的兄弟节点，则将target节点合并到当前节点，
                if let Some(child) = bucket.nodes.borrow_mut().get_mut(&inode.pgid) {
                    child.parent().unwrap().remove_child(child.clone());
                    child.node_mut().parent = Some(WeakNode(Arc::downgrade(&self.0))); //重新计算其父节点为当前节点
                                                                            //将child加入当前node的子节点中
                    child
                        .parent()
                        .unwrap()
                        .node_mut()
                        .children
                        .push(child.clone());
                }
            }

            let mut p = self.parent().unwrap();
            // 将目标节点的元素添加到当前节点的元素数组中
            self.node_mut()
                .inodes
                .append(&mut target.node_mut().inodes.drain(..).collect::<Vec<INode>>());
            p.del(target.node().key.as_ref().unwrap()); //将目标节点的key从父节点中移除（target节点和n的父节点是同一个）
            p.remove_child(target.clone()); //从目标节点的父节点的叶子节点中移除目标节点
            bucket.nodes.borrow_mut().remove(&target.node().pgid); //删除当前bucket的节点缓存中的目标节点
            target.free(bucket); //释放target节点占有的页面
        } else {
            {
                //如果target节点是当前节点的左边的兄弟节点，则将当前节点合并到左边的兄弟节点
                for inode in self.node().inodes.iter() {
                    if let Some(child) = bucket.nodes.borrow_mut().get_mut(&inode.pgid) {
                        child.parent().unwrap().remove_child(child.clone());
                        child.node_mut().parent = Some(WeakNode(Arc::downgrade(&target.0)));
                        child
                            .parent()
                            .unwrap()
                            .node_mut()
                            .children
                            .push(child.clone());
                    }
                }
            } //将当前节点重父节点和当前bucket的缓存中移除，并且将当前节点的元素添加到左边的兄弟节点中
            let mut p = self.parent().unwrap();
            target
                .node_mut()
                .inodes
                .append(&mut self.node_mut().inodes.drain(..).collect::<Vec<INode>>()); // inodes按照key排序，添加到目标节点中仍然是有序的
            p.del(self.node().key.as_ref().unwrap());
            p.remove_child(self.clone());
            bucket.nodes.borrow_mut().remove(&self.node().pgid);
            self.free(bucket);
        }
        self.parent().unwrap().rebalance(page_size, b)
    }


    fn split(&self, page_size: usize, fill_percent: f64) -> Vec<Node> {
        let mut nodes = vec![self.clone()];
        let mut node = self.clone();
        while let Some(b) = node.split_two(page_size, fill_percent) {
            nodes.push(b.clone());
            node = b;
        }
        nodes
    }

    fn split_index(&self, threshold: usize) -> (usize, usize) {
        let mut index: usize = 0;
        let mut sz: usize = 0;
        let n = self.node();
        let max = n.inodes.len() - MIN_KEY_PERPAGE;
        let nodes = &n.inodes;
        for (i, node) in nodes.iter().enumerate().take(max) {
            index = i;
            let elsize = self.page_element_size() + node.key.len() + node.value.len();
            if i > MIN_KEY_PERPAGE && sz + elsize > threshold {
                break;
            }
            sz += elsize;
        }
        (index, sz)
    }

    fn split_two(&mut self, page_size: usize, mut fill_percent: f64) -> Option<Node> {
        if self.node().inodes.len() <= MIN_KEY_PERPAGE * 2 || self.node_less_than(page_size) {
            return None;
        }

        if fill_percent < MIN_FILL_PERCENT {
            fill_percent = MIN_FILL_PERCENT;
        } else if fill_percent > MAX_FILL_PERCENT {
            fill_percent = MAX_FILL_PERCENT;
        }
        let threshold = (page_size as f64 * fill_percent) as usize;
        let (split_index, _) = self.split_index(threshold);

        let next = NodeInner::new().leaf(self.node().is_leaf).build();
        next.node_mut().inodes = self.node_mut().inodes.drain(split_index..).collect();
        Some(next)
    }

    fn node_less_than(&self, v: usize) -> bool {
        let mut sz = PAGE_HEADER_SIZE;
        let elsz = self.page_element_size();
        let a = self.node();
        for i in 0..a.inodes.len() {
            let item = a.inodes.get(i).unwrap();
            sz += elsz + item.key.len() + item.value.len();
            if sz >= v {
                return false;
            }
        }
        return true;
    }

    pub(crate) fn spill(&self, atx: Tx, bucket: &Bucket) -> Result<Node> {
        if self.node().spilled {
            return Ok(self.clone());
        }

        self.node_mut()
            .children
            .sort_by(|a, b| (*a).node().inodes[0].key.cmp(&(*b).node().inodes[0].key));

        let children = self.node().children.clone();
        for child in children.iter() {
            child.spill(atx.clone(), bucket)?;
        }

        self.node_mut().children.clear();

        let mut tx = atx.clone();
        let db = tx.db().unwrap();

        let mut nodes = self.split(PAGE_SIZE, bucket.fill_percent);

        // 这里设置父节点信息
        let parent_node = 
        if nodes.len() == 1 {
            if let Some(p) = &nodes.first().unwrap().node().parent {
                p.upgrade()
            } else {
                None
            }
        } else {
            if let Some(parent) = &self.node().parent {
                let p = parent.upgrade().unwrap();
                for n in nodes.iter_mut() {
                    n.node_mut().parent = Some(parent.clone());
                }
                p.node_mut().children.extend_from_slice(&nodes[1..]);
                Some(p)
            } else {
                let parent = NodeInner::new().leaf(false).build();
                parent
                    .node_mut()
                    .children
                    .extend_from_slice(nodes.as_slice());
                Some(parent)
            }
        };


        for n in nodes.iter_mut() {
            if n.node().pgid > 0 {
                db.0.freelist
                    .try_write()
                    .unwrap()
                    .free(tx.0.meta.borrow().txid, unsafe { &*db.0.page(n.node().pgid) });
                n.node_mut().pgid = 0;
            }

            let mut p = db.0.allocate(n.size() / PAGE_SIZE as usize + 1)?;
            let page = p.to_page_mut();
            let new_id = page.id;
            n.node_mut().pgid = page.id;
            n.write(page);
            tx.0.pages.borrow_mut().insert(page.id, p);
            n.node_mut().spilled = true;

            if let Some(parent) = &parent_node {
                // let mut parent_node = parent.upgrade().map(Node).unwrap();
                if let Some(key) = &n.node().key {
                    let pgid = n.node().pgid;
                    parent.put(key, key, &vec![], pgid);
                } else {
                    let n1 = n.node();
                    let inode = n1.inodes.first().unwrap();
                    let pgid = n.node().pgid;
                    parent.put(&inode.key, &inode.key, &vec![], pgid);
                }
                if n.node().parent.is_none() {
                    n.node_mut().parent.replace(WeakNode(Arc::downgrade(&parent.0)));
                }
            }
        }

        if let Some(mut p) = parent_node {
            p.node_mut().children.clear();
            return p.spill(atx, bucket);
        }

        if let None = parent_node {
            tx.0.meta.borrow_mut().root = self.node_mut().pgid;

        } 
        
        return Ok(self.clone());
    }

}


#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use crate::db::DBInnerState;

    use super::*;
    #[test]
    fn test_node_new() {
        let mut buf = vec![0u8; PAGE_SIZE];
        let mut node1 = NodeInner::new().leaf(true).build();
        node1.put(b"aaa", b"aaa", b"001", 0 );
        node1.put(b"bbb", b"bbb", b"002", 0);
        let page = 
        Page::page_in_buffer_mut(&mut buf, 0);
        node1.write(page);
        let mut node2 = NodeInner::new().leaf(true).build();
        node2.read(page);
        for n in node2.node().inodes.iter() {
            print!(
                "key:{:?},value:{:?},pgid:{} || ",
                n.key, n.value, n.pgid
            );
        }
    }
}
