from deltalake import DeltaTable

def load_table(path: str) -> DeltaTable:
    return DeltaTable(path)
