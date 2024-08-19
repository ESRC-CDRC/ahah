import pandas as pd
from tqdm import tqdm
from ukroutes.common.utils import Paths
from ukroutes.routing import Route

postcodes = pd.read_parquet(Paths.PROCESSED / "onspd" / "all_postcodes.parquet")
nodes = pd.read_parquet(Paths.PROCESSED / "oproad" / "nodes.parquet")
edges = pd.read_parquet(Paths.PROCESSED / "oproad" / "edges.parquet")

pq_files = list((Paths.PROCESSED / "guardian").glob("*.parquet"))
for file in tqdm(pq_files):
    outfile = Paths.OUT / "guardian" / f"{file.stem}_distances.parquet"
    if outfile.exists():
        continue
    source = pd.read_parquet(file).dropna(subset=["easting", "northing"])
    route = Route(source=source, target=postcodes, nodes=nodes, edges=edges)
    distances = route.route()
    distances.to_parquet(outfile)
