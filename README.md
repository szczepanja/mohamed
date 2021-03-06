# Exercise (Mohamed): Using upper Standard Function

Develop a standalone Spark SQL application (using IntelliJ IDEA) that converts one or more string columns to upper case.

The standalone application should take at least two input parameters:

    The path of a CSV data set to load
    One or more column names

The output dataset should extend the current columns with new ones with their names including the “source”, e.g. if city
column were used, the output could be upper_city.

Extra: Make sure the conversion happens on string columns only

Module: Spark SQL

Duration: 30 mins Input Dataset

    +---+-----------------+-------+
    | id|             city|country|
    +---+-----------------+-------+
    |  0|           Warsaw| Poland|
    |  1|Villeneuve-Loubet| France|
    |  2|           Vranje| Serbia|
    |  3|       Pittsburgh|     US|
    +---+-----------------+-------+

CSV file:

    id,city,country
    0,Warsaw,Poland
    1,Villeneuve-Loubet,France
    2,Vranje,Serbia
    3,Pittsburgh,US

Result

    +---+-----------------+-------+-----------------+
    | id|             city|country|       upper_city|
    +---+-----------------+-------+-----------------+
    |  0|           Warsaw| Poland|           WARSAW|
    |  1|Villeneuve-Loubet| France|VILLENEUVE-LOUBET|
    |  2|           Vranje| Serbia|           VRANJE|
    |  3|       Pittsburgh|     US|       PITTSBURGH|
    +---+-----------------+-------+-----------------+

Useful Links

    Scaladoc of the functions object
