// CREATE TABLE IF NOT EXISTS Power_Consumption (
//     experiment_id VARCHAR,
//     node_id VARCHAR,
//     node_id_seq VARCHAR,
//     time_sec VARCHAR NOT NULL,
//     time_usec VARCHAR NOT NULL,
//     power REAL NOT NULL,
//     current REAL NOT NULL,
//     voltage REAL NOT NULL
// );

// COPY Power_Consumption FROM '/home/chris/duckdb-oml-extension/handin/st_lrwan1_11.oml'
// (AUTO_DETECT TRUE);

// CREATE SEQUENCE IF NOT EXISTS Power_Consumption_id_seq;
// CREATE VIEW PC AS (
//     SELECT nextval('power_consumption_id_seq') AS id, cast(time_sec AS real) + cast(time_usec AS real) AS ts, power, current, voltage
//     FROM power_consumption
// );

#define DUCKDB_EXTENSION_MAIN

#include "oml_parser_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

// Copied from read_csv.cpp
#include "duckdb/function/table/read_csv.hpp"

// Read file
#include <fstream>
#include <string>

// from dbgen.cpp
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

// Misc imports
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb
{

  static void CreatePowerConsumptionTable(ClientContext &context, string catalog_name, string schema, string table)
  {
    // CREATE TABLE IF NOT EXISTS Power_Consumption (
    //     experiment_id VARCHAR,
    //     node_id VARCHAR,
    //     node_id_seq VARCHAR,
    //     time_sec VARCHAR NOT NULL,
    //     time_usec VARCHAR NOT NULL,
    //     power REAL NOT NULL,
    //     current REAL NOT NULL,
    //     voltage REAL NOT NULL
    // );
    auto info = make_uniq<CreateTableInfo>();
    info->schema = schema;
    info->table = table;
    info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    info->temporary = false;
    info->columns.AddColumn(ColumnDefinition("experiment_id", LogicalType::VARCHAR));
    info->columns.AddColumn(ColumnDefinition("node_id", LogicalType::VARCHAR));
    info->columns.AddColumn(ColumnDefinition("node_id_seq", LogicalType::VARCHAR));
    info->columns.AddColumn(ColumnDefinition("time_sec", LogicalType::VARCHAR));
    info->columns.AddColumn(ColumnDefinition("time_usec", LogicalType::VARCHAR));
    info->columns.AddColumn(ColumnDefinition("power", LogicalType::DOUBLE));
    info->columns.AddColumn(ColumnDefinition("current", LogicalType::DOUBLE));
    info->columns.AddColumn(ColumnDefinition("voltage", LogicalType::DOUBLE));

    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(3)));
    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(4)));
    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(5)));
    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(6)));
    info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(7)));
    auto &catalog = Catalog::GetCatalog(context, catalog_name);
    catalog.CreateTable(context, std::move(info));
  }

  // static void CreatePCView(ClientContext &context, string catalog_name, string schema, string table)
  // {
  //   // This doesnt work
  //   // How do we specify where to populate the data from? Is it an sql statement we have to write?

  //   // CREATE SEQUENCE IF NOT EXISTS Power_Consumption_id_seq;
  //   // CREATE VIEW PC AS (
  //   //     SELECT nextval('power_consumption_id_seq') AS id, cast(time_sec AS real) + cast(time_usec AS real) AS ts, power, current, voltage
  //   //     FROM power_consumption
  //   // );
  //   auto info = CreateViewInfo();
  //   info.schema = schema;
  //   info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
  //   info.temporary = false;
  //   info.view_name = table;
  //   info.aliases = {"PC"};
  //   info.types = {LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE};

  //   Catalog::GetCatalog(context, catalog_name).CreateView(context, info);
  // }

  pair<int, child_list_t<Value>> ParseOMLHeader(string filename)
  {
    std::ifstream file(filename);
    std::string line;
    int tsv_start_row = 0;

    auto linefeed = [&]() -> string
    {
      std::getline(file, line);
      tsv_start_row++;
      return line;
    };
    linefeed(); // protocol: 5
    linefeed(); // domain: 375823
    linefeed(); // start-time: 1689001665
    linefeed(); // sender-id: st_lrwan1_15
    linefeed(); // app-name: control_node_measures

    // "schema: name1:type1 name2:type2" -> vector<"name:type">
    // Later we parse "name:type" to a pair<"name", Value("type")>
    vector<string> schema_vector; // we still need to turn this into pairs

    while (linefeed().find("schema: ") == 0) // schema: 0 _experiment_metadata subject:string key:string value:string
    {
      vector<string> vector = StringUtil::Split(line, " ");
      vector.erase(vector.begin(), vector.begin() + 3);                        // https://stackoverflow.com/questions/7351899/remove-first-n-elements-from-a-stdvector
      schema_vector.insert(schema_vector.end(), vector.begin(), vector.end()); // https://stackoverflow.com/a/3177254
    }

    // Parse "name:type" to a pair<"name", Value("type")>
    // This is turned to a STRUCT, which the CSV reader uses to parse the column schema
    child_list_t<Value> column_types;
    for (string &name_type_pair : schema_vector)
    {
      vector<string> pair = StringUtil::Split(name_type_pair, ":");
      string name = pair[0];
      string type = pair[1];

      type = StringUtil::Replace(type, "int32", "integer");
      // ... add more types if needed

      column_types.emplace_back(std::make_pair(name, Value(type)));
    }

    linefeed(); // content: text
    D_ASSERT(linefeed().empty());
    return std::make_pair(tsv_start_row, column_types);
  }

  static unique_ptr<FunctionData> OmlGenBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names)
  {
    string filename = input.inputs[0].ToString();

    input.named_parameters["sep"] = Value("\t");
    input.named_parameters["parallel"] = false;

    pair<int, child_list_t<Value>> header = ParseOMLHeader(filename);
    input.named_parameters["skip"] = Value::BIGINT(header.first);
    input.named_parameters["columns"] = Value::STRUCT(header.second);

    // Use default csv implementation with new parameters
    return ReadCSVTableFunction::GetFunction().bind(context, input, return_types, names);
  }

  static void OmlGenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    string catalog_name("memory");
    string schema("main");
    string table("power_consumption");

    CreatePowerConsumptionTable(context, catalog_name, schema, table);
    // CreatePCView(context, catalog_name, schema, table);

    // Use the default csv implementation with new parameters
    ReadCSVTableFunction::GetFunction().function(context, data_p, output);

    auto &catalog = Catalog::GetCatalog(context, catalog_name);
    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, schema, table);
    auto appender = make_uniq<InternalAppender>(context, tbl_catalog);

    // Remember, CSV (DataChunk) has columns
    //    subject:string key:string value:string timestamp_s:uint32 timestamp_us:uint32 power:double voltage:double current:double
    // But table has schema
    //     experiment_id:VARCHAR, node_id:VARCHAR, node_id_seq:VARCHAR, time_sec:VARCHAR NOT NULL, time_usec:VARCHAR NOT NULL, power:REAL NOT NULL, current:REAL NOT NULL, voltage:REAL NOT NULL
    // We must map the columns to the schema using (time)

    vector<string> cols = data_p.bind_data->Cast<ReadCSVData>().return_names;
    for (auto i = 0; i < output.size(); i++)
    {
      auto AppendByName = [&](string column)
      {
        auto it = find(cols.begin(), cols.end(), column);
        D_ASSERT(it != cols.end());
        int col_idx = std::distance(cols.begin(), it);
        appender->Append(output.GetValue(col_idx, i));
      };

      appender->BeginRow();
      appender->Append("experiment_id"); // What should this be?
      appender->Append("node_id");       // What should this be?
      appender->Append("node_id_seq");   // What should this be?
      AppendByName(string("timestamp_s"));
      AppendByName(string("timestamp_us"));
      AppendByName(string("power"));
      AppendByName(string("current"));
      AppendByName(string("voltage"));
      appender->EndRow();
    }
    appender->Close();
  }

  TableFunction OmlGenTableFunction::GetFunction()
  {
    // TableFunction OmlGen("OmlGen", {LogicalType::VARCHAR}, OmlGenFunction, OmlGenBindFunction);
    TableFunction OmlGen(ReadCSVTableFunction::GetFunction());
    OmlGen.name = "OmlGen";
    OmlGen.function = OmlGenFunction;
    OmlGen.bind = OmlGenBindFunction;
    return OmlGen;
  }

  static void LoadInternal(DatabaseInstance &instance)
  {
    ExtensionUtil::RegisterFunction(instance, OmlGenTableFunction::GetFunction());
  }

  void OmlParserExtension::Load(DuckDB &db)
  {
    LoadInternal(*db.instance);
  }
  std::string OmlParserExtension::Name()
  {
    return "oml_parser";
  }

} // namespace duckdb

extern "C"
{

  DUCKDB_EXTENSION_API void oml_parser_init(duckdb::DatabaseInstance &db)
  {
    LoadInternal(db);
  }

  DUCKDB_EXTENSION_API const char *oml_parser_version()
  {
    return duckdb::DuckDB::LibraryVersion();
  }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
