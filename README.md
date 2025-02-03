# M1 : Serialization / Deserialization

## Summary

In M1, I implemented a custom serialization and deserialization schema that should be adaptive and cover many of the ES5 constructs. My implementation contain 1 software component, the `serialization.js` file. This totaled to around 100 lines of code.

### Schema
The serialization and deserialization scheme needed to be designed to be compatible with the `distribution` library for the CS1380 course. Therefore, we defined the serialized scheme to be
```JSON
{type: <object_type>, value: <serialized_value>}
```
The key implementation details rest in the `type` attribute which allows the deserializiation routine to dynamically figure out the type of the object at runtime. Furthermore, this implementation allows us to implement recursive serialization, where the value attribute can too contain its own dictionary mapping serialization objects. 

### Challenges
Some of the challenges I faced with serialization was figuring out which attributes to obtain to properly serialize error objects. It took me a moment to figure out how to properly nest all of the necessary attributes. My implementation for `object` serialization is to create a `serialized` dictionary which each object type interacts with a little differently, such as the recurive nature for `dictionary` and `array` objects.

Some challenges I faced during deserialization was function deserialization. It was fairly easy to obtain the string representation of a function, but in order to decode it, I also tried to use the `Function` constructor. However, I ran into errors as passing in the serialized value of the function resulted in the entirely of the value, including the arrow and declaration, being treated as the function body. This was not correct so the functions were not properly deserialized. I ultimately eneded up using `eval('(' + <func> ')')`, which properly deserializes the function.

## Correctness
I wrote 5 extra tests located in the `m1.student.js` file. These tests assess the edge case robustness of the serialization and deserialization routines. The edge cases span nested dictionaries and functions. It also tests different number types such as `NaN` and `Infinity`. I also wrote the 5 scenario test cases in the `m1.scenario.js` file. These tests also aim to span various cases for the routines such as specfic sized objects and invalid inputs. These tests take `0.453` seconds to run in total. 

## Performance
The latency of the serialization and deserializtion routines is located in the `package.json` file. In the `latency` section, I report the average latency times for the routines to run on three different workloads: small, medium, and large. The average latency is defined as the total amount of time it takes to run the serialization routine on the input 1000 times, divided by 1000 to get the average time. The small workload is the latency to serialize the base object types: `string`, `number`, `boolean`, `null`, and `undefined`. The medium workload is a dictionary with differnt functions such as the fibonacci sequency and arrow functions. Finally, the large workload is just a large dictionary with many different datatypes and a large array. I wrote a function to run each routine 1000 times and I report the average times. 