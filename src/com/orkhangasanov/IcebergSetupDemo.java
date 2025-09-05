package com.orkhangasanov;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

public class IcebergSetupDemo {
    public static void main(String[] args) {
        // Define warehouse path (local filesystem)
        String warehousePath = "file:///tmp/iceberg_warehouse";

        // Initialize Hadoop Catalog
        Catalog catalog = new HadoopCatalog(new Configuration(), warehousePath);

        // Define schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "age", Types.IntegerType.get())
        );

        // Unpartitioned spec
        PartitionSpec spec = PartitionSpec.unpartitioned();

        TableIdentifier tableId = TableIdentifier.of("default", "people");

        Table table;
        if (!catalog.tableExists(tableId)) {
            table = catalog.createTable(tableId, schema, spec);
            System.out.println("Table created: " + table.name());
        } else {
            table = catalog.loadTable(tableId);
            System.out.println("Table loaded: " + table.name());
        }

        System.out.println("Schema: " + table.schema());
        System.out.println("Location: " + table.location());
    }
}




