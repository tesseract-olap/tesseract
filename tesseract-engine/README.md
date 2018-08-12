# tesseract engine

Tesseract engine is a library for performing online analysis on in-memory data. Its logical model is of cubes containing dimensions and measures, which allows for live aggregations and cuts across many axes of analysis.

Tesseract engine is loosely modeled after the ROLAP system of Mondrian with (Postgres | Monet) as the backend. However, tesseract focuses on bringing another level of performance and stability to open source OLAP software.

The biggest trade-off when comparing tesseract to a system such as Mondrian is the lack of database backend. This trade-off was made for several reasons:
- Tesseract focuses on medium-data aggregations: blazing speed for tables in the 10-100 million row range.
- Modern hardware can hold several 100-million row data sets in memory, on basically commodity hardware, obviating the need for all the complexities of streaming from disk.
- Tesseract focuses on infrequently updated data sets, obviating the need for all the complexities of transactions and ACID.

In addition, tesseract should gain performance where it can optimize for aggregations, by tailoring data structures and algorithms (joining and grouping) on data of a specific shape: wide fact tables and dimension tables of low cardinality.

# Architecture

Tesseract engine contains:
- a request handler.
- a query api (but no natural query language, e.g. sql or mdx! this may be provided by another tesseract crate).
- a query executor
- a schema describing relationships within a cube, including dimensions and measures, as well as relationships between a cube and data.
- an in-memory backend

# Physical layer
- dimensions should be stored as a custom Map type with no hashing (basically a Vec).
- in the fact tables
  - dims must be usize.
  - measures should preference being usize, then f64, then String.

# Query lifecycle
- user creates a Query struct containing drilldowns, cuts, and measures, along with some options like parent.
- user sends Query struct to tesseract-engine.
- tesseract-engine request scheduler receives the Query.
- request scheduler assigns the Query to an Executor.
- (multi-threaded, the Executor gets spawned on a threadpool)
- Executor asks Schema for information about the cube in the Query.
- Executor uses info from the cube and the Query to generate a QueryPlan (state machine?).
- Executor gets a reference to the fact table and necessary dims.
- Executor 

# note
columns are stream? Compressed as streams? (the struct could be a combination of null bitvec and vec chunks, with metadata on null ranges, and present an iterator interface)
