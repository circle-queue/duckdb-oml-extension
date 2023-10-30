#pragma once

#include "duckdb.hpp"

namespace duckdb
{
	struct OmlGenTableFunction
	{
		static TableFunction GetFunction();
	};

	class OmlParserExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
	};

} // namespace duckdb
