# models

This module is used for abstracting complex data types and containers with complex instance methods.

Examples of complex data type include `Indicator` (trade signal output), and `FutureTrade`. `FrequencySignal` is an example of a container, which serves to wrap multiple `Indicator` objects.

To prevent circular references, only `primitives` and `core` should be the only dependencies. Both modules should not be referencing any objects defined in this module. However, `strategy` is dependent on `models`.
