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

namespace duckdb
{
  TableFunction ReadOMLTableFunction::GetFunction()
  {
    TableFunction read_oml(ReadCSVTableFunction::GetFunction());
    read_oml.name = "read_oml";
    // Assign a lambda function which overrides the csv bind function
    // Conceptually, it just modifies the input arguments to the default csv bind function
    // Modified parameters: sep, columns, skip
    read_oml.bind = [](ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData>
    {
      ////////////////
      // Assign="sep"
      ////////////////
      std::string sep_str("sep");
      input.named_parameters[sep_str] = Value("\t");

      string filename = input.inputs[0].ToString();

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

      ////////////////
      // Assign="columns"
      ////////////////
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
      std::string cols_str("columns");
      input.named_parameters[cols_str] = Value::STRUCT(column_types);

      linefeed(); // content: text
      D_ASSERT(linefeed().empty());

      ////////////////
      // Assign="skip"
      ////////////////
      std::string skip_str("skip");
      input.named_parameters[skip_str] = Value::BIGINT(tsv_start_row);

      // Use default csv implementation with new parameters
      return ReadCSVTableFunction::GetFunction().bind(context, input, return_types, names);
    };
    return read_oml;
  }

  static void LoadInternal(DatabaseInstance &instance)
  {
    ExtensionUtil::RegisterFunction(instance, ReadOMLTableFunction::GetFunction());
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
