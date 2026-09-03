import os
import polars as pl
import numpy as np

_LOCATIONS = None
_POP_WEIGHTS = None
_CITIES = None

def get_location_sampler():
    global _LOCATIONS, _POP_WEIGHTS, _CITIES
    if _LOCATIONS is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "assets", "demographic_data", "locations_partitions.csv")
        # Read the CSV as a single column and split
        df = pl.read_csv(csv_path, has_header=True)
        # Assuming format: City|State|Zip|Lat|Lon|Population
        parsed = df.select(
            pl.col("output").str.split_exact("|", 5).struct.rename_fields(
                ["city", "state", "zip", "lat", "lon", "population"]
            )
        ).unnest("output")
        
        parsed = parsed.with_columns([
            pl.col("population").cast(pl.Float64, strict=False).fill_null(1.0),
            pl.col("lat").cast(pl.Float64, strict=False),
            pl.col("lon").cast(pl.Float64, strict=False)
        ]).drop_nulls(subset=["city", "state", "lat", "lon"])
        
        _CITIES = parsed.to_dicts()
        pop = parsed["population"].to_numpy()
        _POP_WEIGHTS = pop / pop.sum()
        
    return _CITIES, _POP_WEIGHTS

def sample_locations(n: int, rng: np.random.Generator):
    cities, weights = get_location_sampler()
    indices = rng.choice(len(cities), size=n, p=weights)
    return [cities[i] for i in indices]
