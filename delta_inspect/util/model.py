from pydantic import BaseModel

NUMERIC = int | float


class Histogram(BaseModel):
    """Class for holding histogram information"""
    
    bins: list[NUMERIC]
    cnts: list[int]

class BaseDistribution(BaseModel):
    """Base class for holding distribution information"""
    count: int
    mean: NUMERIC
    std: NUMERIC | None

    min: NUMERIC
    q05: NUMERIC
    q25: NUMERIC
    q50: NUMERIC
    q75: NUMERIC
    q95: NUMERIC
    max: NUMERIC

    hist: Histogram
