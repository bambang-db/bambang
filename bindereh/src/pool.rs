// Buffer pool, act as cache layer for struct Page
pub struct Pool {
    max_pages: usize,
}

impl Pool {
    pub fn new() {}

    pub fn get_page(&self, page_id: u64) -> Page {
        todo!()
    }

    pub fn put_page(&self, page: Page) {
        todo!()
    }
}
