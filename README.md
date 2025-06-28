## Bambang DB

[Muhammad Ilzam Mulkhaq](https://www.linkedin.com/in/ilzam-mulkhaq/) undergraduate thesis aims to study the database domain. Originally, the idea was simply to create a clone of SQLite, but using B+ Tree and introducing parallel (multi-threaded) based execution. This was also inspired by the paper 'SQLite: Past, Present and Future,' which shows that SQLite has poor performance on the SSB Benchmark (OLAP) but great performance on TATP (OLTP), while DuckDB has poor performance on the latter. Basically, this project aims to create HTAP based on OLTP—in short, an OLTP system that has good enough OLAP performance.

'Bambang' in Javanese can mean brave or knight, so the name was taken from the author's bravery in choosing a topic that is very uncommon at the university level, aiming to build enough reputation to secure a master's degree scholarship (perhaps someday). This project is written in Rust, and in this workspace, it will be separated into 3 projects:

### Diplomat

A public facing, here the sql is parsed an turned onto AST, and have some validation, like is the table exist, is the column exist, is the
column have a right data types, etc..

### Pambudi

Query engine, take AST transoform logical plan, have some optimization, then convert-it into a physical plan

### Bindereh

A storage engine, where B+ Tree, introduce parallel scan and partitioning {insert novelty here}