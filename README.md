A single file crappy SQL database in Rust

How this storage paging system should hopefully work:

create root catalog page -> create b tree -> create table_name:root_index entry in catalog page using btree root node -> create a heap page -> write a record to the heap page -> create a record id {heap_page_id, slot} -> call btree.insert(key, rid)

when selecting it should be like:
find root for table in catalog-> traverse b tree until find record id for entry ->
get the record from the heap page
