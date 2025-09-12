#include <esp_jsondb/db.h>
#include <esp_jsondb/utils/refs.h>

void setup() {
    Serial.begin(115200);

    if (!db.init("/refs_db").ok()) {
        Serial.println("DB init failed");
        return;
    }

    JsonDocument author;
    author["name"] = "John Doe";
    auto ar = db.create("authors", author.as<JsonObjectConst>());
    if (!ar.status.ok()) {
        Serial.printf("Author create failed: %s\n", ar.status.message);
        return;
    }

    DocRef ref{"authors", ar.value};
    JsonDocument book;
    book["title"] = "Example Book";
    JsonObject refObj = book["author"].to<JsonObject>();
    refObj["collection"] = ref.collection;
    refObj["_id"] = ref.id;
    auto br = db.create("books", book.as<JsonObjectConst>());
    if (!br.status.ok()) {
        Serial.printf("Book create failed: %s\n", br.status.message);
        return;
    }

    auto fr = db.findById("books", br.value);
    if (fr.status.ok()) {
        auto populated = fr.value.populate("author");
        if (db.lastError().ok()) {
            Serial.printf("Author: %s\n", populated["name"].as<const char*>());
        }
    }

    db.dropCollection("books");
    db.dropCollection("authors");
}

void loop() {
}
