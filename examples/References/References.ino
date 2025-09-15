#include <esp_jsondb/db.h>
#include <esp_jsondb/utils/refs.h>

void setup() {
    Serial.begin(115200);

    if (!db.init("/refs_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    JsonDocument authorDoc;
    authorDoc["name"] = "John Doe";
    auto authorCreateRes = db.create("authors", authorDoc.as<JsonObjectConst>());
    if (!authorCreateRes.status.ok()) {
        Serial.printf("Author create failed: %s\n", authorCreateRes.status.message);
        return;
    }

    DocRef authorRef{"authors", authorCreateRes.value};
    JsonDocument book;
    book["title"] = "Example Book";
    JsonObject authorRefObj = book["author"].to<JsonObject>();
    authorRefObj["collection"] = authorRef.collection;
    authorRefObj["_id"] = authorRef.id;
    auto bookCreateRes = db.create("books", book.as<JsonObjectConst>());
    if (!bookCreateRes.status.ok()) {
        Serial.printf("Book create failed: %s\n", bookCreateRes.status.message);
        return;
    }

    auto bookFindRes = db.findById("books", bookCreateRes.value);
    if (bookFindRes.status.ok()) {
        auto populated = bookFindRes.value.populate("author");
        if (db.lastError().ok()) {
            Serial.printf("Author: %s\n", populated["name"].as<const char*>());
        }
    }

    db.dropCollection("books");
    db.dropCollection("authors");
}

void loop() {
}
