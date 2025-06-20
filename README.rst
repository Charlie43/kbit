====
kbit
====

General process:

- Decided to try out DuckDB
- Lost track of time playing around with the features
- ???
- Panic

I did most of the logic in SQL, apart from one where I used Python (when I was still playing around with DuckDB)

Overall nothing too complicated, I get a list of order ids for each DQ test and output them at the end.

Next steps:

- Handle empty DQ tests
- Split each DQ test into a function to make it all easily testable
- Write tests, generate test data via AI probably
- Make better use of DuckDB or just switch over to the Postgres connector directly as I'm not really using any DuckDB features atm
- Performance improvements: materialise intermediate steps, better filtering for predicate pushdown




Features
--------

* Analyze orders prints out a summary into the console

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
