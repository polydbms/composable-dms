### Components

The modular components each serve one specific purpose and implement their respective interface.
The following comprises a comprehensive overview of the components currently implemented and their 
specifics.

#### Type: Parser 

##### Ibis
Ibis is a python dataframe library that natively offers an interface to connect to various backends with one python 
dataframe API. Here, the Ibis component implements the parser interface and provides Substrait generation based on 
its pythonic way of defining queries. This component does not optimize the queries, but relies on good query structuring 
skills using the pythonic advantages. An exemplary query can be provided as:
```python
query = (
    employees.join(salaries, "Name")
    .filter(_.Department != "IT")
    .group_by("City")
    .agg(mean_salary = _.Salary.mean())
)
```

#### Type: Parser & Optimizer
These components combine the parser and the optimizer interface by providing the parser interface combined with 
optimizations and a soon to come single optimizer implementation.

##### DuckDB 
This component implements DuckDBs rule- and cost-based optimizer together with its SQL parsing capabilities into well 
optimized Substrait plans.

##### DataFusion
This component implements DataFusions rule-based optimizer together with its SQL parsing capabilities into well 
optimized Substrait plans.

##### Calcite
This component implements Calcites cost- and rule-based optimizer together with its SQL parsing capabilities into well 
optimized Substrait plans. It's integrated through the Substrait Java library Isthmus. Specifics include the isthmus JAR 
and additional schema definitions located at `compodb-core/src/substrait_producer/(isthmus_kit/jars)` as well as the jpype 
interface setup between python and java at `compodb-core/src/substrait_producer/java_definitions.py`.

#### Type: Execution Engine

##### DuckDB
This component implements the execution engine interface and allows the use of the push-based vectorized execution engine 
of DuckDB.

##### DataFusion
This component implements the execution engine interface and allows the use of Apache DataFusions extensible query engine.

##### Acero
This component implements the execution engine interface and allows the use of Aceros streaming query engine.


