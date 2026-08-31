#include "mongo_batch_sink.hpp"
#include "mongo_instance.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/insert.hpp>
#include <cstdlib>

namespace duckdb {

MongoBatchSink::MongoBatchSink(idx_t batch_size_p) : batch_size(batch_size_p) {
}

bool MongoBatchSink::Stage(bsoncxx::document::value doc) {
	buffer.push_back(std::move(doc));
	return buffer.size() >= batch_size;
}

bool MongoBatchSink::Empty() const {
	return buffer.empty();
}

idx_t MongoBatchSink::BufferedCount() const {
	return buffer.size();
}

void MongoBatchSink::Flush(mongocxx::collection &collection) {
	if (buffer.empty()) {
		return;
	}
	// Unordered inserts (ordered:false) for throughput: a batch that hits a duplicate _id inserts every other document
	// and then throws. On partial failure, the durable set is the batch minus the rejected documents. The uncaught
	// exception aborts the statement with no count reported.
	collection.insert_many(buffer, mongocxx::options::insert {}.ordered(false));
	buffer.clear();
}

void MongoBatchSink::EnsureCollection(const string &connection_string, const string &database_name,
                                      const string &collection_name) {
	GetMongoInstance();
	auto client = mongocxx::client(mongocxx::uri(connection_string));
	auto database = client[database_name];
	if (!database.has_collection(collection_name)) {
		try {
			database.create_collection(collection_name);
		} catch (const mongocxx::exception &) {
			// A concurrent create_collection race is harmless; the first insert_many will use whatever exists.
		}
	}
}

idx_t MongoResolveBatchSize() {
	idx_t batch_size = 1000;
	const char *env_batch = std::getenv("MONGO_INSERT_BATCH_SIZE");
	if (env_batch != nullptr) {
		auto parsed = std::atoi(env_batch);
		if (parsed > 0) {
			batch_size = NumericCast<idx_t>(parsed);
		}
	}
	return batch_size;
}

} // namespace duckdb
