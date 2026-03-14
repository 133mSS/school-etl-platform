from sqlalchemy import inspect
from src.config.database import warehouse_engine

tables = inspect(warehouse_engine).get_table_names()
print("Warehouse tables:")
for t in sorted(tables):
    print(" ", t)