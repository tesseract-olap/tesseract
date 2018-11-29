# tesseract core

Tesseract core is a library of core componentsfor performing online analysis on in-memory data. Its logical model is of cubes containing dimensions and measures, which allows for live aggregations and cuts across many axes of analysis.


Tesseract core is a thin library at the moment:
- `Schema` definition
- `Query` definition

It's up to each application to implement as appropriate (for now, until I think of a better architecture).
