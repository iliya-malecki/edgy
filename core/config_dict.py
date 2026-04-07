from __future__ import annotations


class ConfigDict:
    """
    A mechanism for configuring topics. The reason it is in a separate class
    is that topics are already too clever with a very particular flat hierarchy expectations,
    i wanted to escape into a normal instance system as fast as possible.
    Essentially the same situation as what is happening in pydantic
    """

    ...
