cmake_minimum_required(VERSION 3.5)

idf_component_register(
    SRCS
        "src/esp_jsondb/db.cpp"
        "src/esp_jsondb/collection/collection.cpp"
        "src/esp_jsondb/document/document.cpp"
        "src/esp_jsondb/utils/objectId.cpp"
    INCLUDE_DIRS "src"
    REQUIRES arduino ArduinoJson
)
