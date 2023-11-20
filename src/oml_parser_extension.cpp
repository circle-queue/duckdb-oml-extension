#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "oml_parser_extension.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fstream>
#include <openssl/opensslv.h>
#include <string>

namespace duckdb
{
  static void CreateTableFromCSVMetadata(ClientContext &context, string catalog_name, string schema, string table, std::vector<string> cols, std::vector<LogicalType> dtypes)
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
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    for (auto idx = 0; idx < cols.size(); idx++)
    {
      info->columns.AddColumn(ColumnDefinition(cols[idx], dtypes[idx]));
      if (idx >= 3)
        info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(idx)));
    }

    auto &catalog = Catalog::GetCatalog(context, catalog_name);
    catalog.CreateTable(context, std::move(info));
  }

  pair<uint64_t, child_list_t<Value>> ParseOMLHeader(string filename)
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

    string catalog_name("memory");
    string schema("main");
    string table("power_consumption");

    auto return_value = ReadCSVTableFunction::GetFunction().bind(context, input, return_types, names);
    vector<string> cols = return_value->Cast<ReadCSVData>().return_names;
    vector<LogicalType> dtypes = return_value->Cast<ReadCSVData>().return_types;
    // Use default csv implementation with new parameters
    return return_value;
  }

  static unique_ptr<FunctionData> PowerConsumptionBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names)
  {
    // Basically uses the general OmlGenBindFunction but overrides the columns with the hardcoded names
    auto return_value = OmlGenBindFunction(context, input, return_types, names);
    //     experiment_id:VARCHAR, node_id:VARCHAR, node_id_seq:VARCHAR, time_sec:VARCHAR NOT NULL, time_usec:VARCHAR NOT NULL, power:REAL NOT NULL, current:REAL NOT NULL, voltage:REAL NOT NULL
    std::vector<string> cols = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
    std::vector<string> types = {"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "REAL", "REAL", "REAL"};

    child_list_t<Value> column_types;
    for (auto i = 0; i < cols.size(); i++)
      column_types.emplace_back(std::make_pair(cols[i], Value(types[i])));
    input.named_parameters["columns"] = Value::STRUCT(column_types);
    return return_value;
  }

  static void OmlGenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
  {
    vector<string> cols = data_p.bind_data->Cast<ReadCSVData>().return_names;

    // Use the default csv implementation with new parameters
    ReadCSVTableFunction::GetFunction().function(context, data_p, output);

    string catalog_name("memory");
    string schema("main");
    string table("power_consumption");

    auto &catalog = Catalog::GetCatalog(context, catalog_name);
    auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, schema, table);
    auto appender = make_uniq<InternalAppender>(context, tbl_catalog);

    // Remember, CSV (DataChunk) has columns
    //    subject:string key:string value:string timestamp_s:uint32 timestamp_us:uint32 power:double voltage:double current:double
    // But table has schema
    //     experiment_id:VARCHAR, node_id:VARCHAR, node_id_seq:VARCHAR, time_sec:VARCHAR NOT NULL, time_usec:VARCHAR NOT NULL, power:REAL NOT NULL, current:REAL NOT NULL, voltage:REAL NOT NULL
    for (auto i = 0; i < output.size(); i++)
    {
      appender->BeginRow();
      for (auto j = 0; j < cols.size(); j++)
        appender->Append(output.GetValue(j, i));
      appender->EndRow();
    }
    appender->Close();
  }

  static unique_ptr<GlobalTableFunctionState> ReadOMLInitGlobal(ClientContext &context, TableFunctionInitInput &input)
  {
    vector<string> cols = input.bind_data->Cast<ReadCSVData>().return_names;
    vector<LogicalType> dtypes = input.bind_data->Cast<ReadCSVData>().return_types;

    string catalog_name("memory");
    string schema("main");
    string table("power_consumption");

    auto &catalog = Catalog::GetCatalog(context, catalog_name);
    CreateTableFromCSVMetadata(context, catalog_name, schema, table, cols, dtypes);
    return ReadCSVTableFunction::GetFunction().init_global(context, input);
  }

  static void LoadInternal(DatabaseInstance &instance)
  {
    TableFunction Power_Consumption_load(ReadCSVTableFunction::GetFunction());
    Power_Consumption_load.name = "Power_Consumption_load";
    Power_Consumption_load.function = OmlGenFunction;
    Power_Consumption_load.bind = PowerConsumptionBind;
    ExtensionUtil::RegisterFunction(instance, Power_Consumption_load);

    TableFunction OmlGen = ReadCSVTableFunction::GetFunction();
    OmlGen.name = "OmlGen";
    OmlGen.function = OmlGenFunction;
    OmlGen.bind = OmlGenBindFunction;
    OmlGen.init_global = ReadOMLInitGlobal;
    ExtensionUtil::RegisterFunction(instance, OmlGen);
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
