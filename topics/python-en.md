## [Main title](../README.md)
# Python

## Table of Contents
- [1. What is Python?](#1-what-is-python)
- [2. What is PEP?](#2-what-is-pep)
- [3. What are the main elements of the Python engine (using CPython as an example)?](#3-what-are-the-main-elements-of-the-python-engine-using-cpython-as-an-example)
- [4. What is a compiled programming language and an interpreted one? What's the difference?](#4-what-is-a-compiled-programming-language-and-an-interpreted-one-whats-the-difference)
- [5. How does an interpreted programming language work?](#5-how-does-an-interpreted-programming-language-work)
- [6. What is bytecode?](#6-what-is-bytecode)
- [7. Types of typing](#7-types-of-typing)
- [8. What type of typing is used in Python?](#8-what-type-of-typing-is-used-in-python)
- [9. What is a variable](#9-what-is-a-variable)
- [10. Numbers in Python](#10-numbers-in-python)
- [11. Strings in Python](#11-strings-in-python)
- [12. Lists and tuples in Python. What's the difference?](#12-lists-and-tuples-in-python-whats-the-difference)
- [13. Dictionary in Python](#13-dictionary-in-python)
- [14. Name mutable and immutable objects](#14-name-mutable-and-immutable-objects)
- [15. Explain how memory management is implemented in Python.](#15-explain-how-memory-management-is-implemented-in-python)
- [16. What is an allocator?](#16-what-is-an-allocator)
- [17. Type conversion](#17-type-conversion)
- [18. What is type annotation](#18-what-is-type-annotation)
- [19. What is iteration](#19-what-is-iteration)
- [20. What loops are there in Python](#20-what-loops-are-there-in-python)
- [21. What's the difference between while and for](#21-whats-the-difference-between-while-and-for)
- [22. What is the range function](#22-what-is-the-range-function)
- [23. What does enumerate do?](#23-what-does-enumerate-do)
- [24. What is list comprehension in Python?](#24-what-is-list-comprehension-in-python)
- [25. What are the list methods](#25-what-are-the-list-methods)
- [26. What in Python is not an object](#26-what-in-python-is-not-an-object)
- [27. Variable names](#27-variable-names)
- [28. How many key-value pairs can be in a dictionary](#28-how-many-key-value-pairs-can-be-in-a-dictionary)
- [29. What data type can be a value in a dictionary](#29-what-data-type-can-be-a-value-in-a-dictionary)
- [30. How to find out the id of an element](#30-how-to-find-out-the-id-of-an-element)
- [31. What is an iterator in Python?](#31-what-is-an-iterator-in-python)
- [32. What is a generator in Python?](#32-what-is-a-generator-in-python)
- [33. What's the difference between `[x for x in y]` and `(x for x in y)` in Python?](#33-whats-the-difference-between-x-for-x-in-y-and-x-for-x-in-y-in-python)
- [34. What is object-oriented programming (OOP)? Name the main principles of OOP](#34-what-is-object-oriented-programming-oop-name-the-main-principles-of-oop)
- [35. What is a class?](#35-what-is-a-class)
- [36. How many parents and children can a class have](#36-how-many-parents-and-children-can-a-class-have)
- [37. What is MRO?](#37-what-is-mro)
- [38. What is the diamond problem in OOP?](#38-what-is-the-diamond-problem-in-oop)
- [39. What is operator overloading](#39-what-is-operator-overloading)
- [40. Magic methods](#40-magic-methods)
- [41. Access modifiers](#41-access-modifiers)
- [42. Generators](#42-generators)
- [43. What is Comprehension in Python?](#43-what-is-comprehension-in-python)
- [44. Tell us about list generators (list comprehension)](#44-tell-us-about-list-generators-list-comprehension)
- [45. What are mixins](#45-what-are-mixins)
- [46. Star syntax (Add example)](#46-star-syntax-add-example)
- [47. What is a thread and a process?](#47-what-is-a-thread-and-a-process)
- [48. What does "switching" (context switch) between processes and threads mean?](#48-what-does-switching-context-switch-between-processes-and-threads-mean)
- [49. Is Python single-threaded or multi-threaded?](#49-is-python-single-threaded-or-multi-threaded)
- [50. What is GIL](#50-what-is-gil)
- [51. What is asynchronicity and how is it implemented in Python?](#51-what-is-asynchronicity-and-how-is-it-implemented-in-python)
- [52. SOLID principles](#52-solid-principles)
- [53. Can you explain the thread lifecycle?](#53-can-you-explain-the-thread-lifecycle)
- [54. What is a function?](#54-what-is-a-function)
- [55. What is recursion?](#55-what-is-recursion)
- [56. What does the zip() function do?](#56-what-does-the-zip-function-do)
- [57. Tell us about try, raise and finally.](#57-tell-us-about-try-raise-and-finally)
- [58. What happens if you don't handle an error in the except block?](#58-what-happens-if-you-dont-handle-an-error-in-the-except-block)
- [59. Explain the difference between deep copy and shallow copy.](#59-explain-the-difference-between-deep-copy-and-shallow-copy)
- [60. Can we say that a NumPy array is better than a list?](#60-can-we-say-that-a-numpy-array-is-better-than-a-list)
- [61. Can dynamic module loading be implemented in Python?](#61-can-dynamic-module-loading-be-implemented-in-python)
- [62. What methods/functions do we use to determine the type of an instance and inheritance?](#62-what-methodsfunctions-do-we-use-to-determine-the-type-of-an-instance-and-inheritance)
- [63. What is a decorator?](#63-what-is-a-decorator)
- [64. What is a context manager in Python?](#64-what-is-a-context-manager-in-python)
- [65. What is ORM?](#65-what-is-orm)


## 1. What is Python?

**Python** is a high-level interpreted general-purpose programming language created by Guido van Rossum in 1991.  
It focuses on syntax simplicity, code readability, and rapid application development.

### Main Characteristics
- **Interpreted** — code is executed line by line by an interpreter (CPython, PyPy, Jython, etc.).
- **Dynamic typing** — variable type is determined at runtime.
- **High-level** — abstracts low-level details (e.g., memory management).
- **Object-oriented** — everything in Python is an object (including functions and modules).
- **Cross-platform** — works on Windows, Linux, macOS, and many other systems.

### Where it's used
- **Web development**: Django, Flask, FastAPI.
- **Analytics and Data Science**: NumPy, Pandas, Matplotlib, scikit-learn.
- **Machine learning and AI**: TensorFlow, PyTorch.
- **Automation and DevOps**: scripts, testing, CI/CD.
- **GameDev and graphics**: Pygame, Blender API.
- **System utilities and scripts**: quick CLI tool development.

### Pros
- Code simplicity and readability.
- Huge number of libraries and community.
- Versatility: suitable from scripts to large systems.
- Active development and support.

### Cons
- Speed lower than C/C++ (compiled language).
- Higher memory consumption.
- Not always suitable for real-time systems.

### Summary
**Python** is a universal, readable, and flexible programming language used in many areas: from scientific computing and machine learning to web development and automation.

## 2. What is PEP?
PEP - Python Enhancement Proposals - the database of all proposals on how to improve Python and what to change. For example, PEP8 is the generally accepted guide for writing Python code.

## 3. What are the main elements of the Python engine (using CPython as an example)?

### Interpreter (CPython)
- The most popular Python implementation.
- Written in **C**.
- Executes Python code by converting it to **bytecode** (`.pyc`), which is executed by a virtual machine (Python Virtual Machine).

### Private Heap (Python heap)
- All memory for Python objects is stored in a private area (private heap).
- Managed by **Python Memory Manager**.
- Inside there are **allocators** and **arenas** that distribute memory for objects.

### Built-in C libraries
- Like in Node.js, Python has modules written in **C for speed**.
- For example:
   - `math` → uses C functions `libm`.
   - `json` → fast C parser (`_json`).
   - `re` → some functions in C (`_sre`).
   - `io`, `zlib`, `hashlib` and many others.
- These modules are directly connected to CPython as "C extensions".

### Python Standard Library
- Huge set of modules in Python (and partially in C).
- For example: `os`, `sys`, `datetime`, `subprocess`, `threading`.
- Essentially — an analog of "basic APIs" in Node.js.

### C API (Python/C API)
- CPython provides an API for writing your own modules in C.
- This is like Node.js having the ability to write **native addons**.
- Allows integrating Python with high-performance C/C++ code.

### Python Virtual Machine (PVM)
- Executes Python bytecode.
- Analog of "execution engine" in V8.
- Works on top of the interpreter and memory manager.

### Alternative Python implementations
- **PyPy** → JIT compilation, faster.
- **Jython** → Python on JVM.
- **IronPython** → Python on .NET CLR.

### Summary
Yes, Python has a structure similar to Node.js + V8:

- **CPython interpreter** (analog of V8).
- **C libraries and modules** (like Node.js built-in C libraries).
- **Python Standard Library** (like core modules in Node.js).
- **C API for extensions** (analog of native addons in Node.js).

📌 The difference is that Node.js = V8 + libuv (focused on asynchronicity), while Python = CPython + C libraries + rich standard library.

## 4. What is a compiled programming language and an interpreted one? What's the difference?

### Compiled language
- Program code is pre-translated by a compiler into **machine code** (executable binary file).
- After that, the program runs directly by the OS and processor.
- Examples: **C, C++, Rust, Go**.

**Pros:**
- High execution speed.
- Fewer runtime dependencies.

**Cons:**
- Longer development cycle (need to compile).
- Less flexible for quick changes.

### Interpreted language
- Program code is executed **line by line** by an interpreter.
- The interpreter analyzes, parses, and executes code in real time.
- Examples: **Python, JavaScript, Ruby, PHP**.

**Pros:**
- Fast development and debugging.
- Portability: code can run immediately on different platforms (if there's an interpreter).

**Cons:**
- Slower than compiled languages.
- Requires an interpreter in the runtime environment.

### Summary
- **Compiled languages** → run fast, but require compilation before running.
- **Interpreted languages** → simpler and faster to develop, but run slower.

📌 In practice, many modern languages use a **hybrid approach**:
- Java and C# → compilation to bytecode + interpretation/Just-In-Time (JIT) compilation.
- Python → first bytecode `.pyc`, then execution by interpreter (CPython).

## 5. How does an interpreted programming language work?

### General idea
- In interpreted languages, code is **not fully compiled to machine code in advance**.
- Instead, the program is executed **line by line** (or in blocks) through an **interpreter**, which:
   1. Reads source code.
   2. Converts it to internal representation (e.g., bytecode).
   3. Executes instructions through a virtual machine.

### Execution stages (using Python / CPython as an example)

1. **Parsing**
   - Source code → *Abstract Syntax Tree (AST)*.
   - Syntax is checked (`SyntaxError` if there's an error).

2. **Compilation to bytecode**
   - Python compiles code to **bytecode** (`.pyc` files), which is understood by Python Virtual Machine (PVM).
   - This is an intermediate step (code closer to machine, but not yet native).

3. **Execution (Interpretation)**
   - Bytecode is executed by **Python Virtual Machine (PVM)**.
   - PVM converts bytecode instructions into specific operations for the processor.

📌 So Python is not a "purely interpreted language", but **compiled-interpreted**:  
Source `.py` → compilation to bytecode `.pyc` → bytecode interpretation by PVM.

### Example (Python)
Source code:
```python
   print("Hello, world!")
```
Execution path:
1. Interpreter parses the line.
2. Python compiler creates bytecode (e.g., `LOAD_GLOBAL print; LOAD_CONST "Hello"; CALL_FUNCTION`).
3. Python Virtual Machine (PVM) executes these instructions.
4. `"Hello, world!"` appears on screen.

## 6. What is bytecode?
Python is an interpreted programming language. It doesn't convert its code to machine code that hardware understands (unlike C and C++). The process of such conversion is called compilation. Instead, the Python interpreter, or more precisely its standard implementation CPython, translates program code into bytecode, which runs on Python Virtual Machine (PVM). There's a Python interpreter implementation that works through JIT (just in time) compilation - PyPy.

## 7. Types of typing

### By timing of type checking
 - **Static** - typing where a variable is bound to a type at declaration time, and the type cannot be changed later. Example of static typing (C++):
```
int x = 5;
x = "abc";  // here C++ compiler will complain
```
or, the same thing:
```
int x;
x = 5;
x = "abc"; // here C++ compiler will complain
```

- **Dynamic** - typing where a variable's type is set at value assignment time, not at declaration time, and thus can be changed later.
Example of dynamic typing (Python):
```
x = 5
x = "a"  # here interpreter doesn't complain, as typing is dynamic
```

### By strictness of control
- **Strict** - absence of automatic casts to another type (implicit conversions). Example of strict typing (Python):
```
a = [5, 6]
print(",".join(a))  # here interpreter complains, as join() expects a list of strings as input
```
- **Non-strict** - language allows automatic type conversions. Example of non-strict typing (Javascript):
```
let a = "hello";
let b = 100;
let c = a + b;
console.log(c);  // "hello100"
```
### By mutability of types
- **Explicit** - we specify types everywhere manually. Example of explicit typing (C++):
```
int x = 5;
y = 6;  // here compiler will complain
```
- **Implicit** - language determines type based on value. Example of implicit typing (Python):
```
a = 1
```
**Important**: memory is used most rationally and optimally in case of strict static typing.

## 8. What type of typing is used in Python?

Dynamic, strict, implicit.

## 9. What is a variable
A Python variable is an identifier for referencing an object in program memory. Multiple variables can point to (reference) the same object in memory. As soon as variables stop referencing a memory area - that area is cleaned by the garbage collector, and thus memory is freed through counting references to objects in memory.

## 10. Numbers in Python
- int - integers.
- float - real or floating-point numbers.
- complex - complex numbers.
- decimal - decimal fractions.

## 11. Strings in Python
A string is an ordered sequence of characters designed to store information in the form of simple text. In Python3, a string by default has Unicode encoding, which eliminates problems with working and displaying Cyrillic characters and other exotic encodings. A string is an immutable data type, i.e., if you need to add characters to an existing string, you'll have to create a new string with a new memory address:
```
a = "hello"
id(a) # 2044344987401
a = "hello world"
id(a) # 2044334957804
```

## 12. Lists and tuples in Python. What's the difference?
Python lists are similar to arrays in other languages. A tuple is similar to a list, but you create it with parentheses instead of square brackets. You can also use the built-in tool to create tuples. The difference between lists and tuples is that a tuple is immutable, while a list is mutable. I.e., you can add an element to a list and its memory address won't change, but if you need to add an element to a tuple, you need to create a new tuple, and it will have a new memory address:
```
# list
a = [1,2]
id(a) # 2044364987904
a.append(3)
id(a) # 2044364987904

# tuple
a = (1,2)
id(a) # 2044370285184
a = (1,2,3)
id(a) # 2044369999872
```
**Important**: a tuple takes up less memory space than a list, so whenever an array of objects is known to be immutable, it's recommended to use tuples instead of lists.

## 13. Dictionary in Python
Dictionaries in Python are collections of arbitrary objects with key access. Starting from Python3.6, the dict() dictionary is ordered, i.e., when iterating through an existing dictionary, elements are returned in the order they were added to the dictionary when it was populated. Before Python3.6, you had to use the OrderedDict() object to have an ordered collection with key access to elements. A dictionary is a mutable data type.

## 14. Name mutable and immutable objects
### Mutable
Objects whose **content** (data inside) can be changed, while the **reference to the object remains the same**. That is, the object in memory retains its `id()`, but its internal state changes.  
- Examples: list, dict and set.

### Immutable
Objects whose content cannot be changed after creation. Any "attempt to change" leads to creating a **new object** in memory with a different `id()`.
- Examples: int, float, bool, string and tuple.

## 15. Explain how memory management is implemented in Python.

### General principle
- In Python **all objects and data structures are stored in a private dynamic memory area (private heap)**.
- This area is managed by the **Python interpreter**, specifically — **Python Memory Manager**.
- The user **has no direct access** to this area and doesn't manage it manually — they only work with references to objects.

### Python Memory Manager
- Responsible for allocating and freeing memory for all objects.
- Delegates part of the work to specialized **allocators**, which are assigned to specific object types (e.g., lists, strings).
- Monitors that memory usage doesn't exceed the allocated private heap.

➡ Essentially, when we write `x = [1, 2, 3]`, the interpreter allocates memory in private heap, the memory manager distributes needed blocks, and we get only a reference to the object.

### Reference counting
- Each object in Python stores a reference counter.
- When a reference to an object is created → counter increases.
- When a reference is lost (`del`, exiting scope) → counter decreases.
- If counter = 0 → object is immediately freed from memory.

### Garbage collector
- Reference counting doesn't handle circular references (objects reference each other).
- For this, there's a built-in **garbage collector** (`gc`), which:
   - periodically traverses objects,
   - looks for cycles,
   - frees their memory.
- Collection is organized by generations (Generational GC):
   - **Gen 0** — young objects (short-lived).
   - **Gen 1 and Gen 2** — older objects, checked less frequently.

### Object caching
For optimization, Python caches some objects:
- small integers (`-5 ... 256`),
- strings (string interning),
- sometimes — recently created objects.  
  This speeds up work and reduces memory consumption.

### Manual management (rarely needed)
Although the process is fully automated, a programmer can "peek" or influence:

```python
   import sys, gc

   a = [1, 2, 3]
   print(sys.getrefcount(a))  # number of references
   gc.collect()               # manual garbage collector run
```
But in real practice, this is usually not necessary.

### Summary
- In Python, memory for objects is stored in **private heap**, managed by **Python Memory Manager**.
- Developer works only with **references to objects**, not with memory blocks themselves.
- Memory management is built on:
   - **reference counting**,
   - **garbage collector for cycles**,
   - **interpreter optimizations (object caching, generational GC)**.
- User doesn't manage memory directly — the interpreter handles this.


## 16. What is an allocator?

**Allocator (memory allocator)** is a component of the memory manager that **allocates and manages memory blocks** for program objects.  
It decides:
- which memory area to take from the heap,
- how to distribute it for an object,
- when this area can be freed.

### Allocator in Python
Python has **its own memory allocation system on top of malloc/free** from C.

- **Python Memory Manager** distributes memory within **private heap**.
- For different objects (e.g., `int`, `list`, `dict`, `str`), specialized **allocators** are used:
   - **PyObject allocator** — allocates memory for Python objects themselves.
   - **Block allocators** (with pools) — manage small memory chunks (e.g., for lists).
   - **Arena allocator** — allocates large memory chunks (arenas) and breaks them into blocks.

Thus, instead of asking the OS for memory every time, Python uses **pools and arenas**, which greatly speeds up work.

### Example from CPython
- Python stores memory in fixed-size blocks (usually 8 KB).
- Arena is broken into pools (4 KB each).
- Pools contain blocks for objects of the same size.
- Allocator tracks which blocks are occupied and which are free.

📌 For example: when you create a list `[1,2,3]`, memory for the list itself and elements is taken not directly from the OS, but from a pool managed by the allocator.

### Why is this needed?
- Reduces the number of OS calls (expensive operation).
- Reduces memory fragmentation.
- Makes Python work more predictably in terms of performance.

### Summary
An allocator in Python is a mechanism within the memory manager that manages **allocation and freeing of memory blocks for objects**.  
Thanks to allocators, Python works faster and more efficiently than if each object requested memory directly from the OS.

## 17. Type conversion
- Type conversion is the conversion of an object from one data type to another data type.
- Implicit type conversion is automatically performed by the Python interpreter.
- Python allows avoiding data loss in implicit type conversion.
- Explicit type conversion is also called type casting, object data types are converted using a predefined function.
- During type casting, data loss can occur, as we cast an object to a specific data type.

## 18. What is type annotation
In the simplest case, annotation contains the expected type directly. Variable annotations are written with a colon after the identifier. After that, value initialization can follow. For example `price: int = 5`
Function parameters are annotated the same way as variables, and the return value is specified after the arrow `->` and before the closing colon. For example
`def indent_right(s: str, width: int) -> str:`.

**Important**: type annotation is not the same as static typing.

## 19. What is iteration
Moving to the next object in a collection, for example, a list, tuple, etc. The collection object itself must be iterable.

## 20. What loops are there in Python
`while` and `for`.

## 21. What's the difference between while and for
**For** is used only when you need to iterate through elements a predetermined number of times. The **while** loop is also used for repeating parts of code, but instead of looping **n** times, it performs work until it reaches a certain condition. You can exit the loop at any moment using the `break` keyword, and move to the next loop iteration using the `continue` word.

## 22. What is the range function
The `range()` function returns an object with an iterator interface that yields elements from a range defined by the function arguments range(start, stop, step), while not storing all elements in memory.

## 23. What does enumerate do?
If the initial value of the `enumerate()` counter is not passed — it defaults to 0. The function creates an object that generates tuples consisting of the element's index and the element itself.
### Example
```python
    fruits = ["apple", "banana", "cherry"]

    # Example 1: without specifying initial value
    for index, fruit in enumerate(fruits):
        print(index, fruit)
    
    # Output:
    # 0 apple
    # 1 banana
    # 2 cherry
    
    
    # Example 2: with specifying initial counter value
    for index, fruit in enumerate(fruits, start=1):
        print(index, fruit)
    
    # Output:
    # 1 apple
    # 2 banana
    # 3 cherry
```

## 24. What is list comprehension in Python?

**List comprehension** is a concise and convenient way to create lists in Python using expressions inside square brackets.  
It combines a `for` loop, conditional expressions, and calculations.

### General syntax
```python
   [<expression> for <element> in <iterable> if <condition>]
```
- `<expression>` — what will be added to the list.
- `<element>` — loop variable.
- `<iterable>` — list, string, range, generator, etc.
- `if <condition>` — filter (optional).

### Examples

**a) Simple list**
```python
   nums = [x for x in range(5)]
   print(nums)  # [0, 1, 2, 3, 4]
```
**b) With calculation**
```python
   squares = [x*x for x in range(5)]
   print(squares)  # [0, 1, 4, 9, 16]
```
**c) With condition**
```python
   evens = [x for x in range(10) if x % 2 == 0]
   print(evens)  # [0, 2, 4, 6, 8]
```
**d) Nested loops**
```python
   pairs = [(x, y) for x in range(2) for y in range(3)]
   print(pairs)  # [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
```
### Advantages
- More **concise and readable notation**.
- Works **faster** than a regular loop with `.append()`.
- Supports filtering and nested loops.

### Summary
**List comprehension** is an idiomatic way to create lists in Python, allowing you to write compact and expressive code.

## 25. What are the list methods
- list.append(x) Adds an element to the end of the list
- list.extend(L) Extends the list by adding all elements of list L to the end
- list.insert(i, x)	Inserts value x at the i-th element
- list.remove(x) Removes the first element in the list with value x. ValueError if such element doesn't exist
- list.pop([i])	Removes the i-th element and returns it. If index is not specified, the last element is removed
- list.index(x, [start [, end]])	Returns the position of the first element with value x (search is conducted from start to end)
- list.count(x)	Returns the number of elements with value x
- list.sort([key=function])	Sorts the list based on a function
- list.reverse() Reverses the list
- list.copy()	Shallow copy of the list
- list.clear() Clears the list

## 26. What in Python is not an object
In Python, everything is an object, except keywords: in, is, if, while, etc.

## 27. Variable names
A variable name can consist only of digits, letters, and underscores. A variable name can start only with a letter or an underscore. No numbers should be at the beginning of a variable name. A variable name cannot contain Python language keywords (reserved). It's customary to separate words with underscores (snake case).

## 28. How many key-value pairs can be in a dictionary
Limited by the amount of RAM.

## 29. What data type can be a value in a dictionary
A key can be basically any immutable data type.

## 30. How to find out the id of an element
The `id()` function returns a unique identifier for the specified object. Essentially, this identifier uniquely determines the object's address in the interpreter's memory.

### Example
- Different objects have different ids. Each value is stored as a separate object, so their ids are different.
```python
    a = 10
    b = 20
    
    print(id(a))  # e.g.: 140713254780048
    print(id(b))  # e.g.: 140713254780368
```
- Variables with the same value can have the same id. Python optimizes storage of small integers (-5 … 256) and some strings, so such objects can reference the same memory area.
```python
    x = 1000
    y = 1000
    print(id(x))
    print(id(y))
    print(x is y)  # False (usually different objects)
    
    x = 10
    y = 10
    print(id(x))
    print(id(y))
    print(x is y)  # True (small integers are interned)
```
- Checking that variables point to the same object. Here a and b are references to the same object (list).
```python
    a = [1, 2, 3]
    b = a
    
    print(id(a))  # same id
    print(id(b))  # same id
    print(a is b) # True
```
- Creating a copy. In this case, b is a new object, so it has a different id.
```python
    import copy

    a = [1, 2, 3]
    b = copy.copy(a)
    
    print(id(a))  # e.g.: 2109987654320
    print(id(b))  # different id
    print(a is b) # False
```
### When is it useful to use `id()`

- To understand if variables reference the same object.
- For debugging, when you need to track if new objects are created or old ones are reused.
- When working with mutable (`list`, `dict`, `set`) and immutable (`int`, `str`, `tuple`) objects — it's convenient to see how Python manages memory.


## 31. What is an iterator in Python?

**Iterator** is an object in Python that implements the iteration protocol:
- method **`__iter__()`**, returning the iterator itself,
- method **`__next__()`**, returning the next element of the sequence.

When elements are exhausted, `__next__()` raises the **`StopIteration`** exception.

### Difference from iterable
- **Iterable** — an object that can be iterated over (e.g., `list`, `tuple`, `str`, `dict`). It can create an iterator using `iter()`.
- **Iterator** — the object itself that yields elements one by one (through `next()`).

📌 That is: *Iterable is a "container", and Iterator is a "traversal mechanism" for the container.*

### Example
```python
   nums = [1, 2, 3]
   it = iter(nums)   # create iterator from list
   
   print(next(it))   # 1
   print(next(it))   # 2
   print(next(it))   # 3
   print(next(it))   # StopIteration
```

### Custom iterator
You can write your own iterator class by implementing `__iter__` and `__next__`:
```python
   class Counter:
      def __init__(self, limit):
         self.i = 0
         self.limit = limit
         
      def __iter__(self):
         return self

      def __next__(self):
          if self.i < self.limit:
              self.i += 1
              return self.i
          else:
              raise StopIteration
              
   c = Counter(3)
   for x in c:
      print(x)  # 1, 2, 3
```

### Where is iterator used?
- In **`for` loops** — Python automatically calls `iter(obj)` and `next()`.
- In **generators** (`yield`) — Python creates an iterator itself.
- In **list comprehension** and other expressions.
- In file operations (`for line in open("file.txt")`).

### Summary
- Iterator in Python is an object with `__iter__` and `__next__` methods.
- It yields elements one by one and signals the end of the sequence through `StopIteration`.
- All `for` loops and generators use iterators under the hood.

## 32. What is a generator in Python?

**Generator** is an object in Python that simplifies iterator creation.  
It automatically implements the iterator protocol (`__iter__` and `__next__`), but you don't need to write these methods manually.

### How generators are created

#### a) Generator functions
- Use the **`yield`** keyword instead of `return`.
- Each call to `yield` returns a value and "freezes" function execution until the next call.
```python
   def countdown(n):
      while n > 0:
        yield n
        n -= 1

   for x in countdown(3):
      print(x)   # 3, 2, 1
```
- Inside the function there's a while loop that describes the logic of generating numbers.
  - yield instead of return makes the function a generator:
  - on each call, the next value is returned;
  - function execution "freezes" at this point;
  - on the next call, it continues from where it stopped.

👉 Example: countdown(3) creates a generator that can yield 3 → 2 → 1 on demand.
- Why for is needed
  - for in Python can work with iterators and generators.
  - A generator by itself is an object that yields values one by one, but they need to be "extracted".
  - for hides the work with next() under the hood: it takes the next value from the generator until it's exhausted.
- Equivalent of for loop using next():
```python
    gen = countdown(3)
    print(next(gen))  # 3
    print(next(gen))  # 2
    print(next(gen))  # 1
    print(next(gen))  # StopIteration (generator exhausted)
```

#### b) Generator expressions
- Similar to list comprehension, but use parentheses.
```python
   squares = (x*x for x in range(5))
   print(list(squares))  # [0, 1, 4, 9, 16]
```

### Differences from regular functions
- **Regular function** returns a result once and finishes work.
- **Generator** can return values many times, "remembering" its state between calls.

### Generator advantages
1. **Memory savings**
   - Generator doesn't store the entire sequence in memory, but yields elements one by one (lazy evaluation).
   - Useful for large data and infinite sequences.

2. **Simplicity of writing iterators**
   - No need to write `__iter__` and `__next__` manually.

3. **Performance**
   - Generators work faster and more efficiently than creating large lists.

### Example application
- Reading a file line by line:
```python
   def read_file(filename):
      with open(filename) as f:
         for line in f:
            yield line.strip()
```
- Working with infinite sequences:
```python
   def infinite_numbers():
      n = 0
      while True:
         yield n
         n += 1
```

### Summary
- **Generator in Python** is a special type of iterator created by a function with `yield` or a generator expression.
- It yields values one by one, preserving its state.
- Generators are convenient when you need **memory savings** and **lazy evaluation**.

## 33. What's the difference between `[x for x in y]` and `(x for x in y)` in Python?

### `[x for x in y]` — list (list comprehension)
- Creates a **list** immediately with all elements.
- All results are stored in memory simultaneously.
- Can access elements by index.
- Suitable if the result is needed completely and not too large.

**Example:**
```python
   nums = [x*x for x in range(5)]
   print(nums)       # [0, 1, 4, 9, 16]
   print(nums[2])    # 4
```

### `(x for x in y)` — generator (generator expression)
- Creates a **generator** that yields elements one by one when iterated.
- Uses lazy evaluation (**lazy evaluation**) → doesn't store the entire sequence in memory.
- Cannot directly access an element by index.
- Suitable for working with large data or streams.

**Example:**
```python
   nums = (x*x for x in range(5))
   print(nums)       # <generator object …>
   print(list(nums)) # [0, 1, 4, 9, 16]
```

### Comparison

| Characteristic     | `[x for x in y]` (list)     | `(x for x in y)` (generator)     |
|--------------------|-----------------------------|----------------------------------|
| **Result**      | Ready list              | Generator (iterator)             |
| **Computations**     | All elements at once          | Gradually, on demand           |
| **Memory**         | Stores all data           | Saves memory                  |
| **Indexing**     | Yes (`list[0]`)              | No                              |
| **Application**     | Small/medium data        | Large data, lazy streams   |

### Summary
- `[x for x in y]` → list, all data at once in memory.
- `(x for x in y)` → generator, yields data one by one.  
  📌 If you need the **complete result** → list. If you need a **lazy stream** → generator.

## 34. What is object-oriented programming (OOP)? Name the main principles of OOP

**Object-oriented programming (OOP)** is a programming paradigm where a program is built around objects.  
**Object** is an entity that combines:
- **data** (properties, attributes),
- **methods** (behavior, functions).

### Main OOP principles

1. **Encapsulation**
    - Combining data and methods that work with them into a single object.
    - Hiding internal implementation and providing an external interface.
    - Example: `private` attributes, access to them through `get/set` methods.

2. **Inheritance**
    - Ability to create new classes based on existing ones.
    - Simplifies code reuse.
    - Example: `Student` class inherits properties and methods from `Person` class.

3. **Polymorphism**
    - Ability to use the same interface for different types of objects.
    - For example, the `draw()` method can be implemented differently in `Circle`, `Square`, `Triangle`.

4. **Abstraction**
    - Highlighting the main and hiding the unnecessary.
    - Defining interfaces without implementation.
    - Example: abstract class `Shape` with `draw()` method, which is implemented by descendants.

### Example
```python
    class Animal:
        def speak(self):
            raise NotImplementedError("This method needs to be overridden")
    
    class Dog(Animal):
        def speak(self):
            return "Woof!"
    
    class Cat(Animal):
        def speak(self):
            return "Meow!"
    
    animals = [Dog(), Cat()]
    for a in animals:
        print(a.speak())  # Polymorphism: outputs "Woof!" and "Meow!"
```

### OOP pros
- Easier to model real entities.
- Code reuse through inheritance.
- System flexibility and extensibility.
- Encapsulation makes code safer and clearer.

### OOP cons
- Can be excessive for simple tasks.
- Higher complexity than procedural approach.
- Sometimes leads to over-complicated architecture.

## 35. What is a class?
A class is a type that describes the structure of objects. In other words, it's a description of an entity that has a certain set of properties and methods.

## 36. How many parents and children can a class have
Unlimited number.

## 37. What is MRO?

**MRO (Method Resolution Order)** is the order in which Python searches for attributes and methods in the class hierarchy during multiple inheritance.

When a method is called on an object:
1. Python first looks for it in the class itself.
2. Then — in base classes, in a certain order.
3. If not found → in `object` (root class).

### How MRO works
- Python uses the **C3 linearization** algorithm to determine class traversal order.
- It ensures:
   - preservation of inheritance order,
   - predictability of method search,
   - absence of conflicts during multiple inheritance.

### Example
```python
   class A:
      def whoami(self): return "A"
   
   class B(A):
      def whoami(self): return "B"
   
   class C(A):
      def whoami(self): return "C"
   
   class D(B, C):
      pass
   
   d = D()
   print(d.whoami())       # "B"
   print(D.mro()) # [<class '__main__.D'>, <class '__main__.B'>, <class '__main__.C'>, <class '__main__.A'>, <class 'object'>]
```

### Why is MRO needed?
- To avoid the **diamond of death** in inheritance (diamond problem).
- To understand in what order methods will be called (`super()` depends on MRO).
- To debug complex class hierarchies.

### How to view MRO
- Method `Class.mro()`
- Attribute `Class.__mro__`

### Summary
**MRO (Method Resolution Order)** is a Python mechanism that determines the order of searching for methods and attributes in the class hierarchy.  
The C3 linearization algorithm makes this order predictable and prevents conflicts during multiple inheritance.

## 38. What is the diamond problem in OOP?

**Diamond problem** is a situation in multiple inheritance where the same base class is inherited multiple times through different branches of the hierarchy.  
As a result, ambiguity arises: **which version of the method should be called?**

### Example
```python
   class A:
       def hello(self): return "A"

   class B(A):
       def hello(self): return "B"

   class C(A):
       def hello(self): return "C"

   class D(B, C):
       pass

   d = D()
   print(d.hello())   # ??? B or C?
```
Here both `B` and `C` inherit from `A`.  
Class `D` inherits from both (`B` and `C`), so when calling `d.hello()`, the question arises: **take implementation from `B`, from `C`, or from `A`?**

### Solution in Python
Python uses the **C3 linearization algorithm** and method resolution order (MRO).

### Diamond problem in other languages
- In **C++** the diamond problem is solved through the `virtual` keyword for base classes.
- In **Java** and **C#** this problem is avoided, as classes can only inherit from one parent (multiple inheritance is implemented only through interfaces).

### Summary
**Diamond problem** is ambiguity in multiple inheritance when the same base class appears multiple times in the hierarchy.  
In Python, this problem is solved through **MRO and the C3 linearization algorithm**, which makes method search order predictable.

## 39. What is operator overloading
One of the ways to implement polymorphism, which consists in the possibility of simultaneous existence in one scope of several different variants of operator application that have the same name, but differ in the types of parameters to which they are applied. Operator overloading is **not** supported in Python.

Example from C++, where operator overloading is supported:
```
float getArea(float a, float b) // function calculating the area of a rectangle with length a and height b
{
  return a * b;
}

float getArea(float r) // function calculating the area of a circle with radius r
{
  return pi * r * r;
}

float S1 = getArea(5.0, 6.0); // 30
float S3 = getArea(5.0); // 78.5
```

## 40. Magic methods
These are special methods in Python, enclosed in two underscores
- Object initialization: `__init__`
- String representations: `__repr__`, `__str__`
- Iteration: `__iter__`, `__next__` and others

## 41. Access modifiers
There are three types of access modifiers in Python OOP:
- public `public`
- private `__private`
- protected `_protected`

**Important**: in Python there's no strict encapsulation, i.e., even a private method can be accessed from outside the class. Encapsulation in Python is more of an agreement between developers than strict hiding, as in C++ or Java.

## 42. Generators
This is a function that, when called in the **next()** function, returns the next object according to its working algorithm. Instead of the **return** keyword, **yield** is used in a generator. The main difference between yield and return is that yield, after returning an object, preserves the generator's stack, so that on the next call to the next() function from the generator, the generator code execution continues from the moment where yield returned the object last time.

## 43. What is Comprehension in Python?

**Comprehension** is a special syntactic way in Python for concise and convenient generation of collections (lists, sets, dictionaries).  
They allow writing compact code instead of loops and `append`.

### Types of comprehension
1. **List comprehension**
```python
    nums = [x*x for x in range(5)] # [0, 1, 4, 9, 16]
```
2. **Set comprehension**
```python
    nums = {x*x for x in range(5)} # {0, 1, 4, 9, 16}
```
3. **Dict comprehension**
```python
    squares = {x: x*x for x in range(5)} # {0: 0, 1: 1, 2: 4, 3: 9, 4: 16}
```
4. **Generator expression**
```python
    nums = (x*x for x in range(5))
```

### Comprehension advantages
- Conciseness and readability.
- Performance (often faster than loop with `append`).
- Universality: applicable to lists, sets, dictionaries, generators.

### Summary
**Comprehension** in Python is "inclusions" for generating new collections (list, set, dict) or generators.  
They allow writing more compact and expressive code than traditional loops.

## 44. Tell us about list generators (list comprehension)
Generators allow creating lists with one line of code
```
>>> [i for i in range(1, 11, 2)]
[1, 3, 5, 7, 9] 
```

## 45. What are mixins
**Mixin** is a set of properties and methods that can be used in various classes that don't come from a base class.

### Main characteristics of mixins
1. Mixin is a helper class that adds functionality to other classes.
2. Usually mixins have **no state** (or minimal).
3. Mixins are **not used independently**, but only as an "addition" to the main class.
4. This is an alternative to copying code or utility functions.

### Example
```python
    class LoggerMixin:
        def log(self, message):
            print(f"[LOG]: {message}")
    
    
    class Service(LoggerMixin):
        def process(self):
            self.log("Process started")
            print("Doing work...")
            self.log("Process completed")
    
    
    s = Service()
    s.process()
```
- LoggerMixin adds logging capability,
- Service is the main class that "mixes in" logging logic.

### When to use mixins?
- When you need to add the same functionality to different classes.
- When you don't want to duplicate code.
- When functionality is logically separate from the main class responsibility (logging, serialization, validation, etc.).

### But:
- Avoid too complex hierarchy with many mixins (will be hard to read code).


## 46. Star syntax (Add example)
- * and ** for passing arguments to a function;
- * and ** for collecting arguments passed to a function;
- ** for accepting only named arguments;
- * when unpacking tuples;
- * for unpacking iterable objects into a list/tuple;
- ** for unpacking dictionaries into other dictionaries.

## 47. What is a thread and a process?

### Process
- **Process** is a program in execution state.
- Each process has:
    - its own **address space**,
    - its own resources (files, sockets, descriptors),
    - at least one thread (usually — main).
- Processes are isolated from each other: one process cannot directly change another's memory.

📌 In Python, process creation can be done through the `multiprocessing` module.
📌 Example: launched a browser — this is a process. It has memory for tabs, access to file system and network connections.

### Thread
- **Thread** is a "lightweight" sub-process within one process.
- All threads of one process:
    - share common address space and resources,
    - but have **their own stack and registers** (execution context).
- Thread switching is faster than process switching.

📌 In Python, threads are created through the `threading` module.
📌 Example: browser — process, in which:
- one thread handles the interface,
- second loads the page,
- third plays video.

### Comparison

| Criterion              | Process                           | Thread                              |
|-----------------------|-----------------------------------|-------------------------------------|
| **Memory**            | Own address space | Shared process memory               |
| **Isolation**          | Full isolation                   | No isolation (can interfere with each other) |
| **Creation**          | More expensive (OS resources)               | Cheaper, faster                    |
| **Switching**      | Slower                         | Faster                             |
| **Reliability**        | One process crash doesn't break others | Thread crash can damage entire process |
| **Usage**     | CPU-bound tasks, parallelism      | I/O-bound tasks, concurrency    |

### Example in Python
**Thread (threading):**
```python
import threading

def worker():
    print("Thread working")

t = threading.Thread(target=worker)
t.start()
t.join()
```
**Process (multiprocessing):**
```python
from multiprocessing import Process

def worker():
    print("Process working")

p = Process(target=worker)
p.start()
p.join()
```
### Summary
- Process = independent program with its own memory.
- Thread = "lightweight" execution flow within a process that shares common memory.
- Threads → faster, but more complex synchronization.
- Processes → slower, but safer (isolation).

## 48. What does "switching" (context switch) between processes and threads mean?

### General idea
Operating system (OS) manages multitasking.  
When the processor has many tasks (processes/threads), the OS by timers or events **stops execution of one task and transfers control to another**.  
This process is called **context switching**.

### Context
**Context** = execution state of a task:
- processor register values,
- stack pointer,
- program counter (address of next instruction),
- sometimes memory state and descriptors.

### Process switching
- OS must **save full process context** (registers + memory table).
- Context of another process is loaded.
- This is "heavy" switching, because each process has its own **address space**.

📌 Therefore, process switching is more expensive in time and resources.

### Thread switching
- Threads within one process **share common address space and resources**.
- OS saves and restores only **registers and stack of current thread**.
- This is "light" switching, faster than processes.

### Example in practice
- When you run two threads in Python (`threading.Thread`), the OS alternately allocates processor time to them.
- If a thread waits for I/O, control can quickly switch to another thread.
- If computation is happening (CPU-bound), Python with GIL will switch threads, but only one thread executes code simultaneously.

### Summary
- **Switching (context switch)** = OS operation of saving state of one task and restoring another.
- **Between processes** → heavier (need to change memory and resources).
- **Between threads** → lighter (shared memory, only registers/stack are saved).

## 49. Is Python single-threaded or multi-threaded?

### Technically: multi-threaded
- Python has the **`threading`** module that creates **real OS threads** (POSIX threads / Win32 threads).
- Threads actually run in parallel, multiple `Thread` can execute in one Python process.
- You can use `threading`, `concurrent.futures.ThreadPoolExecutor`, etc.

### Limitation: GIL (Global Interpreter Lock)
- In **CPython** (standard Python implementation) there's a global interpreter lock.
- It guarantees that **at any moment only one thread executes Python bytecode**.
- This means:
    - For **I/O-bound** tasks (network, files, DB requests) threads are useful → GIL is often released during waiting.
    - For **CPU-bound** tasks (pure Python computations) threads **don't provide real parallelism**.

### How to bypass GIL limitation
- Use **multiprocessing** (`multiprocessing`, `ProcessPoolExecutor`) → each process has its own GIL, parallelism is real.
- Use **asynchronicity** (`asyncio`) → cooperative multitasking without extra threads.
- Use **C libraries** that release GIL (e.g., NumPy, pandas, OpenCV).

### Summary
- Python **supports multithreading** (has OS threads).
- But in **CPython** due to GIL Python **is not fully multi-threaded for CPU tasks**.
- Therefore, the correct answer:
    - **For I/O — multi-threaded** (efficiently).
    - **For CPU — essentially single-threaded** (need processes or C extensions).

## 50. What is GIL
Global Interpreter Lock of CPython. This is a mechanism that prevents multiple threads from executing the same bytecode. In other words, thanks to GIL, at any moment in the context of one Python process, only one thread (Thread) executes, all others wait for their turn to execute. This is why multithreading doesn't speed up data processing in different threads, as everything is actually processed sequentially.

### Why it's needed
1. **Simplifies memory management**
    - In CPython, memory is managed through **reference counting**.
    - Each object stores the number of references to it.
    - Without GIL, you'd have to make complex and "expensive" locks on every counter update.
    - GIL solves the problem: no need for fine-grained locks → code is simpler and faster.

2. **Simplicity of interpreter implementation**
    - GIL allows making CPython simpler, more stable, and faster in single-threaded scenarios.
    - In the 90s, when CPython was created, multiprocessor machines were rare, and the focus was on simplicity.

3. **Compatibility with C extensions**
    - Many Python modules are written in C (NumPy, sqlite3, hashlib).
    - GIL simplifies integration: extensions can work without worrying about thread safety of Python objects.

### GIL cons
- **No real parallelism for CPU-bound tasks** → threads don't speed up computations.
- Potential locks even for small tasks.
- Hard to scale on multi-core processors.

### Bypassing GIL
- **Multiprocessing** (`multiprocessing`, `ProcessPoolExecutor`).
- **Asynchronicity** (`asyncio`) for I/O.
- **C extensions** (e.g., NumPy temporarily releases GIL during computations).
- In Python 3.13, an **experimental version without GIL** appeared (PEP 703).

### Summary
GIL was made to:
- simplify memory management and reference counting,
- ease integration with C extensions,
- make CPython simpler and faster in single-threaded mode.

But the price of this solution is the absence of real multi-threaded parallelism for CPU tasks.

## 51. What is asynchronicity and how is it implemented in Python?

**Asynchronicity** is a model of concurrent execution where one thread (or a few threads) **switches between tasks** at moments of I/O waiting (network, files, DB) and thus efficiently uses time.  
This is **cooperative multitasking**: a task **yields control itself** when it encounters `await`.

### How this relates to threads and GIL
- Asynchronicity is **not about parallel cores** and doesn't bypass GIL: bytecode is still executed one at a time.
- The gain is that instead of blocking I/O waiting, the **event loop** quickly switches execution to other tasks.

### Implementation in Python: `asyncio`
Basic elements of the ecosystem:

- **Event loop** — the heart of the system, schedules and executes tasks.
- **Coroutines** — functions declared `async def`, which can be paused/resumed.
- **`await`** — yield point: coroutine gives control to the loop while waiting for I/O.
- **Tasks/Futures** — wrappers for scheduling and tracking coroutine completion.
- **Non-blocking I/O** — sockets/files/timers integrated with OS selectors.

Example of minimal code:
```python
    import asyncio

    async def fetch(n):
        print(f"start {n}")
        await asyncio.sleep(1)  # I/O simulation
        print(f"done {n}")
        return n

    async def main():
        tasks = [asyncio.create_task(fetch(i)) for i in range(3)]
        results = await asyncio.gather(*tasks)
        print(results)

    asyncio.run(main())
```
→ All three "requests" execute concurrently in one thread: total ~1 sec, not ~3 sec.

### Where this is used
- High-load network services (HTTP servers, proxies, gateways).
- DB/queue clients (PostgreSQL, Redis, Kafka) with async drivers.
- Web frameworks: **FastAPI**, **aiohttp**, **Quart**.
- Reactors/bots, web socket handling.

### When asynchronicity is especially useful
- Many **simultaneous I/O operations**.
- Tens of thousands of connections on one process.
- Minimizing thread overhead and OS switching.

For **CPU-bound** tasks, asynchronicity doesn't speed up — use processes (`multiprocessing`, `ProcessPoolExecutor`) or C/NumPy that can bypass GIL.

### Typical mistakes and recommendations
- ❌ Running **blocking** operations inside `async` (e.g., `time.sleep`, heavy computations).  
  ✅ Use `await asyncio.sleep(...)` or move to pool:  
  `await asyncio.to_thread(blocking_func, *args)` / `run_in_executor`.
- ❌ Mixing different event loops/double `asyncio.run`.  
  ✅ One loop per process; inside — `create_task`, `gather`.
- ❌ Unhandled exceptions in tasks.  
  ✅ Use `gather(..., return_exceptions=False)` or catch errors.
- ❌ Forgotten task cancellations on exit.  
  ✅ Store task references, cancel correctly (`task.cancel()`).

### Alternatives and accelerators
- **uvloop** — fast event loop implementation on libuv (often gives x2–x4).
- Third Python implementations: **PyPy**, **CPython 3.13 free-threaded** (experimental), but the `asyncio` model remains relevant.

### Summary
Asynchronicity in Python is an **event-driven, cooperative concurrency model** based on `asyncio`: coroutines (`async/await`) + event loop + non-blocking I/O.  
It's ideal for I/O-intensive tasks with many simultaneous operations and is not intended for accelerating pure CPU computations.

## 52. SOLID principles

S – Single Responsibility (Single Responsibility Principle)

```Each class should be responsible for only one operation.```

O — Open-Closed (Open-Closed Principle)

```Classes should be open for extension, but closed for modification.```

L — Liskov Substitution (Liskov Substitution Principle)

```If P is a subtype of T, then any objects of type T present in the program can be replaced with objects of type P without negative consequences for the program's functionality.```

I — Interface Segregation (Interface Segregation Principle)

```Clients should not be forced to depend on methods they don't use.```

D — Dependency Inversion (Dependency Inversion Principle)

```High-level modules should not depend on low-level modules. Both should depend on abstractions. Abstractions should not depend on details. Details should depend on abstractions.```

## 53. Can you explain the thread lifecycle?

In general terms, the cycle looks like this:
- first, a class is created that overrides the execution method of the thread class, and we create an instance for this class;
- we call start(), which prepares the thread for execution;
- we put the thread into execution state;
- we can call different methods, for example sleep() and join(), which put the thread into waiting mode;
- when waiting or execution mode ends, other waiting threads are prepared for execution;
- after execution ends, the thread stops.

## 54. What is a function?

When we want to execute a certain sequence (sequence of statements), we can give it a name. For example, let's define a function that takes two numbers and returns the larger one.

## 55. What is recursion?

This is when a function calls itself. At the same time, it must have a base condition to avoid creating an infinite loop:

## 56. What does the zip() function do?

Returns an iterator with tuples:

In this case, it combines elements of two lists and creates tuples from them. Works not only with lists.

## 57. Tell us about try, raise and finally.

These are keywords for exception handling. Potentially risky code is placed in a try block, the raise statement is used to directly call an error, and the finally block contains code that executes in any case.

## 58. What happens if you don't handle an error in the except block?

If this is not done, the program will terminate. Then it will send the execution trace to sys.stderr.

## 59. Explain the difference between deep copy and shallow copy.

Deep copying creates a new copy object. That is, if you make a change to the copy of an object, nothing will happen to the original object. In Python, this is done using the deepcopy() function by importing from the copy module.

```python
    import copy
    b=copy.deepcopy(a)
    # Shallow copy copies to a new object the reference that is attached to the original object. Therefore, if you make a change to the copy, it will spread to the original object. This functionality is implemented using the copy() function.

    b=copy.copy(a)
```

## 60. Can we say that a NumPy array is better than a list?

NumPy arrays have three advantages over lists:

They are faster
They consume less memory
They are more convenient to work with

## 61. Can dynamic module loading be implemented in Python?

With dynamic loading, modules are loaded only when they become needed. This approach is slow, but it helps use memory more efficiently. In Python, you can use the importlib module for this:

import importlib
module = importlib.import_module('my_package.my_module')

## 62. What methods/functions do we use to determine the type of an instance and inheritance?

For this, type(), isinstance() and issubclass() are used.

1. type() is used to determine the type of an object.

```python
type(3)
type(False)
type(lambda :print("Hi"))
type(type)
```
2. isinstance() takes two arguments: value and type. If the value belongs to the corresponding type, True is returned. If not, False is returned.

```python
isinstance(3,int) # True
isinstance((1),tuple) # False
isinstance((1,),tuple) # True
```
3. issubclass() takes two classes as arguments. If the second inherits from the first, True is returned. If not, False is returned.
```python
class A: pass
class B(A): pass

issubclass(B,A) # True
issubclass(A,B) # False
```

## 63. What is a decorator?

**Decorator** is a function (or class) that takes another function/method/class as an argument and returns a modified object.  
Decorators allow **changing or extending the behavior of functions and classes without changing their code**.

### Syntax
In Python, decorators are applied using the `@` symbol above a function or class.
```python
@decorator_name
def my_function():
    …

#This is equivalent to:

def my_function():
    …
my_function = decorator_name(my_function)
```
### Example
```python
    def my_decorator(func):
        def wrapper():
            print("Before function call")
            func()
            print("After function call")
        return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()
#Output:
#Before function call
#Hello!
#After function call
```
Decorators with arguments
```python
    def repeat(n):
        def decorator(func):
            def wrapper(*args, **kwargs):
                for _ in range(n):
                    func(*args, **kwargs)
            return wrapper
        return decorator

@repeat(3)
def greet(name):
    print(f"Hello, {name}!")

greet("Alice")

#Output:
#Hello, Alice!
#Hello, Alice!
#Hello, Alice!
```

### Examples of built-in decorators
- `@staticmethod` — defines a static method in a class.
- `@classmethod` — method that takes `cls` instead of `self`.
- `@property` — turns a method into an attribute-property.
- `@functools.lru_cache` — caching function results.

### Where decorators are applied
- Logging (log function calls).
- Access control (e.g., in Django).
- Caching (function acceleration).
- Measuring execution time.
- DB transactions.

### Summary
**Decorator** in Python is a convenient way to wrap functions and classes to add new behavior (logging, access control, caching, etc.) without changing their source code.

## 64. What is a context manager in Python?

**Context manager** is an object that manages entry and exit from a certain code block.  
Its main task is to guarantee execution of "finalization" (closing files, freeing resources, releasing locks, etc.), even if an exception occurs inside the block.

Used through the **`with`** construct.

### How it works
Context manager implements two methods:
- `__enter__()` — executed when entering the `with` block. Returns an object that will be bound to the variable after `as`.
- `__exit__(exc_type, exc_val, exc_tb)` — executed when exiting the block (always, even if an error occurred).

### Example: working with a file
```python
    with open("data.txt", "r") as f:
        data = f.read()
```
- `open` returns a file object that has `__enter__` and `__exit__` implemented.
- After exiting the `with` block, the file is automatically closed (`f.close()` is called inside `__exit__`).

### Where context managers are applied
- Working with files (`open`).
- Working with network connections and sockets.
- Managing DB transactions.
- Working with locks in multithreading (`threading.Lock`).
- Temporary environment changes (e.g., `decimal.localcontext()`).

### Summary
**Context manager** in Python is a resource management mechanism that through `with` guarantees correct allocation and freeing of resources.  
It implements `__enter__` and `__exit__` methods, or is created through `@contextmanager`.

## 65. What is ORM?

**ORM (Object-Relational Mapping)** is a technology that allows working with a database through objects and classes instead of writing SQL queries manually.

