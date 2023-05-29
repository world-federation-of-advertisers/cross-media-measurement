# panel-exchange-client

Panel exchange client

# Terminology

## Join Keys

Join keys are previously agreed upon fields used to identify panelist data.
There are a few kinds of join keys:

1.  Plaintext Join Keys

    Keys that have been hashed previous to input to the system.

2.  Single Blinded Join Keys

    Keys that have a single layer of added commutative encryption.

3.  Double Blinded Join Keys

    Single blinded keys that another party has added a second layer of
    commutative encryption on top of.

4.  Lookup Keys

    Double blinded keys that have had the original layer of commutative
    encryption removed and then hashed using an identifier pepper. Lookup keys
    can be used to look up panelist data from a system.
