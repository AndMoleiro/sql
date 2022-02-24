/*

Do some kind of operation or change to each item in an array

+---------------------------------------------------+
|                      syntax                       |
+---------------------------------------------------+
| array_transform(array, 'x:...', operator, operand |
+---------------------------------------------------+

    - Args:

        array -> must be an array

        field -> should either be "x" or "x:subfield" (where subfield is a nested field in each object in a given array of objects)

        operator -> can be concat, precat, *, +, -, /, regex_ext, regex_rm

        operand -> necessary for each operator (should be a string for concat/precat/regex_ext/regex_rm) else a number

    - Operators

        (for an array of strings) needs operand:

            concat: attach a string to the end of each string in the array

            precat: attach a string to the front of each string in the array

            regex_ext: if there is a matching regex in each string, return it else null

            regex_rm: remove all instances of the provided regex from each string in the array

        (for an array of numbers) needs operand

            *, +, -, /: perform the given operation on each number in the array

        (for an array of objects) no operand provided
 */

-- Eg 1:

    select array_transform(array_construct('abcd', 'bcde', 'cdef'), 'x', 'regex_rm', 'cd') as a

        --> ['ab', 'be', 'ef']

-- Eg 2:

        select array_transform( -- first transform to get into top_questions[#]
                   array_flatten( -- flatten the array to be able to transform it further
                           array_transform( -- second transform to get into top_answers[#].content
                                   array_filter( -- for records that are not null
                                           ask_the_community:top_questions, 'x:top_answers', 'not null'),
                                   'x:top_answers')), 'x:content') as my_array,
    from S3_DATA_SOURCES.CARPE_DATASOURCES."s174/data/parquet"
    where my_array <> array_construct()

