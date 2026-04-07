def validate_flat_subclassing(candidate: type, reference: type) -> None:
    for candidate_base in candidate.__bases__:
        if issubclass(candidate_base, reference) and candidate_base is not reference:
            raise ValueError(
                f"Cannot subclass {candidate.__name__}: further subclassing "
                f"of {reference.__name__} subclasses is not allowed"
            )
