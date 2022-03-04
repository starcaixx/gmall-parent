package module;

import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Arrays;

public class ModuleTest1 {
    public static void main(String[] args) throws DatabaseNotExistException, DatabaseAlreadyExistException, DatabaseNotEmptyException, TableAlreadyExistException, TableNotExistException, FunctionNotExistException {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tabEnv = TableEnvironment.create(settings);
        Arrays.stream(tabEnv.listModules()).forEach(System.out::println);

        tabEnv.listFullModules();

//        tabEnv.loadModule("hive",new HiveModule("3.1.2"));
//        tabEnv.listModules();
        Arrays.stream(tabEnv.listFunctions()).forEach(System.out::println);

        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "hive", "");
        tabEnv.registerCatalog("myhive",hiveCatalog);

//        hiveCatalog.createDatabase("mydb",new CatalogDatabaseImpl());
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();
        tabEnv.createTable("myhive.mydb.mytable", TableDescriptor.forConnector("kafka")
                .schema(schema).build());

        hiveCatalog.listTables("mydb");

        tabEnv.executeSql("create database mydb with (...)");
        tabEnv.executeSql("create table mytable (name string,age int) with (...)");
        tabEnv.listTables();


        //catalog api
        hiveCatalog.createDatabase("mydb",null,false);
        hiveCatalog.dropDatabase("mydb",false);
        hiveCatalog.alterDatabase("mydb",null,false);
        hiveCatalog.getDatabase("mydb");
        boolean mydb = hiveCatalog.databaseExists("mydb");
        hiveCatalog.listDatabases();

        //table api
        hiveCatalog.createTable(new ObjectPath("mydb","mytable"),null,false);
        hiveCatalog.dropTable(new ObjectPath("mydb","mytable"),false);
        hiveCatalog.getTable(ObjectPath.fromString("mytable"));
        hiveCatalog.tableExists(ObjectPath.fromString("mytable"));

        //partition api
        /*hiveCatalog.createPartition(
                new ObjectPath("mydb","mytable"),
                new CatalogPartitionSpec(...),
                new CatalogPartitionImpl(...),false
        );*/
//        hiveCatalog.getPartition(new ObjectPath("mydb","mytable"),new CatalogPartitionSpec())
//        hiveCatalog.partitionExists()
//        hiveCatalog.listPartitions()
//        hiveCatalog.listPartitionsByFilter()

        //function api
//        hiveCatalog.createFunction(new ObjectPath("mydb","mytable"),
//                new CatalogFunctionImpl(),false);

        hiveCatalog.dropFunction(new ObjectPath("mydb","mytable"),false);
//        hiveCatalog.alterFunction(new ObjectPath("mydb","mytable"),new CatalogFunctionImpl(...),false);
        hiveCatalog.getFunction(ObjectPath.fromString("myfunc"));
        hiveCatalog.listFunctions("mydb");
        hiveCatalog.functionExists(ObjectPath.fromString("myfunc"));

        //register catalog
//        tabEnv.registerCatalog(new CustomCatalog("mycatalog"));
        tabEnv.useCatalog("");
        tabEnv.useDatabase("");
        tabEnv.from("not_the_current_catalog.not_the_current_db.my_table");
        tabEnv.listDatabases();
        tabEnv.listTables();
        tabEnv.listFunctions();
    }
}
