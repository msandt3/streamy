from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
import os

iceberg_path = os.getcwd() + "/data/wiki_events"
catalog_name = "default"
namespace_name = "wiki_events"

def create_catalog() -> SqlCatalog:
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{iceberg_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{iceberg_path}/kafka_messages/data",
        },
    )
    return catalog

def import_metadata(catalog: Catalog, table_name: str, metadata_path: str) -> None:
    return None

if __name__ == "__main__":
    print(create_catalog())