#include "../utils/refs.h"
#include "dbTest.h"

void DbTester::refPopulateTest() {
	// create author
    JsonDocument authorDoc;
    authorDoc["name"] = "John Doe";
    auto authorCreateRes = db.create("authors", authorDoc.as<JsonObjectConst>());
    if (!authorCreateRes.status.ok()) {
        ESP_LOGE(DB_TESTER_TAG, "Failed to create author. Error: %s", authorCreateRes.status.message);
        return;
    }
    std::string authorId = authorCreateRes.value;

	// create book referencing author
    DocRef authorRef{"authors", authorId};
    JsonDocument book;
    book["title"] = "Example Book";
    JsonObject authorRefObj = book["author"].to<JsonObject>();
    authorRefObj["collection"] = authorRef.collection;
    authorRefObj["_id"] = authorRef.id;
    auto bookCreateRes = db.create("books", book.as<JsonObjectConst>());
    if (!bookCreateRes.status.ok()) {
        ESP_LOGE(DB_TESTER_TAG, "Failed to create book. Error: %s", bookCreateRes.status.message);
        db.dropCollection("authors");
        return;
    }
    std::string bookId = bookCreateRes.value;

	// fetch book and populate author reference
    auto bookFindRes = db.findById("books", bookId);
    if (!bookFindRes.status.ok()) {
        ESP_LOGE(DB_TESTER_TAG, "Failed to find book. Error: %s", bookFindRes.status.message);
    } else {
        auto populatedAuthor = bookFindRes.value.populate("author");
        if (!db.lastError().ok()) {
            ESP_LOGE(DB_TESTER_TAG, "Populate failed: %s", db.lastError().message);
        } else {
            const char *authorName = populatedAuthor["name"].as<const char *>();
            if (authorName && std::string(authorName) == "John Doe") {
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
