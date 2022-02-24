/*

Filter array of objects or native types based on a provided condition

+------------------------------------------------+
|                     syntax                     |
+------------------------------------------------+
| array_filter(array, 'x:...', operator, operand |
+------------------------------------------------+


    - Args:

        array -> must be an array

        field -> should either be "x" or "x:subfield" (where subfield is a nested field in each object in a given array
        of objects). If a subfield is provided, then the filtering logic will occur on the subfield, objects will still
        be returned.

        operator -> can be <, >, <=, >=, =, !=, regex, null, not null

        operand -> necessary for <, >, <=, >=, =, !=, regex

    - Operators

        .for any type - needs operand:

            =, != -> filter out items that are = or != the given operand

        .for numbers - needs operand

            <, >, <=, >= -> return only the numbers for which the given relationship is true

        .for strings - needs operand

            regex -> return only strings that contain a regex match for the given operand

        .for any type - no operand provided

            null or not null -> filter out items for which each item is null or not null

 */

-- Eg 1:

    select array_filter(array_construct('abcd', 'bcde', 'cdef'), 'x', 'regex', 'b')

       --> ['abcd', 'bcde']


-- Eg2:

    select array_filter(reviews, 'x:stars', '=', '4')

        --> [{stars = 4, content = "best food ever!", ... },
        --> {stars = 4, content = "pretty good food", ... },
        --> {stars = 4, content = "pretty good food" ... }]

