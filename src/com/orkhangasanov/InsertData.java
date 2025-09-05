package com.orkhangasanov;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import java.io.File;

public class InsertData {
    public static void main(String[] args) throws Exception {
        String warehousePath = "file:///tmp/iceberg_warehouse";

        // Initialize Hadoop Catalog
        Catalog catalog = new HadoopCatalog(new Configuration(), warehousePath);

        Table table = catalog.loadTable(TableIdentifier.of("default", "people"));

        // Create a simple record
        Record record1 = GenericRecord.create(table.schema());
        record1.setField("id", 3);
        record1.setField("name", "Alice");
        record1.setField("age", 30);

        Record record2 = GenericRecord.create(table.schema());
        record2.setField("id", 4);
        record2.setField("name", "Robert");
        record2.setField("age", 25);

        // Write records into a Parquet file
        File dataFile = new File("/tmp/people-data1.parquet");
        try (FileAppender<Record> writer = Parquet.write(Files.localOutput(dataFile))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build()) {
            writer.add(record1);
            writer.add(record2);
        }

        DataFile icebergDataFile = DataFiles.builder(table.spec())
                .withInputFile(Files.localInput(dataFile))
                .withRecordCount(2)
                .withFormat(FileFormat.PARQUET)
                .build();

        // Commit the file to the table
        table.newAppend().appendFile(icebergDataFile).commit();

        System.out.println("Data inserted into people table");
    }
}
