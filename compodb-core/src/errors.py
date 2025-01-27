

class CompoDBError(Exception):
    """Base class for all CompoDB errors."""
    pass

class SubstraitError(CompoDBError):
    """Raised when there is a substrait specific issue."""
    pass

class ProductionError(SubstraitError):
    """Raised when there is an issue with producing a substrait query."""
    pass

class ConsumptionError(SubstraitError):
    """Raised when there is an issue with executing a substrait query."""
    pass
