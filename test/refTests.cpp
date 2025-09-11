#include "../utils/refs.h"
#include "dbTest.h"

void DbTester::refPopulateTest() {
	// create author
	JsonDocument author;
	author["name"] = "John Doe";
	auto ar = db.create("authors", author.as<JsonObjectConst>());
	if (!ar.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to create author. Error: %s", ar.status.message);
		return;
	}
	std::string authorId = ar.value;

	// create book referencing author
	DocRef authRef{"authors", authorId};
	JsonDocument book;
	book["title"] = "Example Book";
	JsonObject refObj = book["author"].to<JsonObject>();
	refObj["collection"] = authRef.collection;
	refObj["_id"] = authRef.id;
	auto br = db.create("books", book.as<JsonObjectConst>());
	if (!br.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to create book. Error: %s", br.status.message);
		db.dropCollection("authors");
		return;
	}
	std::string bookId = br.value;

	// fetch book and populate author reference
	auto fr = db.findById("books", bookId);
	if (!fr.status.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to find book. Error: %s", fr.status.message);
	} else {
		auto populatedAuthor = fr.value.populate("author");
		if (!db.lastError().ok()) {
			ESP_LOGE(DB_TESTER_TAG, "Populate failed: %s", db.lastError().message);
		} else {
			const char *name = populatedAuthor["name"].as<const char *>();
			if (name && std::string(name) == "John Doe") {
				ESP_LOGI(DB_TESTER_TAG, "Reference populated successfully");
			} else {
				ESP_LOGE(DB_TESTER_TAG, "Reference populated but data mismatch");
			}
		}
	}

	// cleanup collections
	auto st = db.dropCollection("books");
	if (!st.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to drop 'books' collection. Error: %s", st.message);
	}
	st = db.dropCollection("authors");
	if (!st.ok()) {
		ESP_LOGE(DB_TESTER_TAG, "Failed to drop 'authors' collection. Error: %s", st.message);
	}
}
