## Bambang DB

[Muhammad Ilzam Mulkhaq](https://www.linkedin.com/in/ilzam-mulkhaq/) undergraduate thesis aims to study the database domain. Originally, the idea was simply to create a clone of SQLite, but using B+ Tree and introducing parallel (multi-threaded) based execution. This was also inspired by the paper 'SQLite: Past, Present and Future,' which shows that SQLite has poor performance on the SSB Benchmark (OLAP) but great performance on TATP (OLTP), while DuckDB has poor performance on the latter. Basically, this project aims to create HTAP based on OLTP—in short, an OLTP system that has good enough OLAP performance.

'Bambang' in Javanese can mean brave or knight, so the name was taken from the author's bravery in choosing a topic that is very uncommon at the university level, aiming to build enough reputation to secure a master's degree scholarship (perhaps someday). This project is written in Rust, and in this workspace, it will be separated into 3 projects:

### Diplomat

Taken from English, meaning 'a person who can deal with people in a sensitive and effective way,' this library is public-facing. Here, the SQL is parsed and turned into an AST, and includes some validation, such as whether the table exists, whether the column exists, whether the column has the right data types, etc.

### Pambudi

Taken from Javanese, meaning 'one who directs with wisdom,' this is the query engine. It takes the AST, transforms it into a logical plan, applies some optimizations, then converts it into a physical plan.

### Bindereh

Taken from Madurese, meaning 'nobility,' this is the storage engine where the B+ Tree is implemented, introducing parallel scan and partitioning.