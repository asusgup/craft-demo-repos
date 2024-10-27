SCHEMA_STR = {
  "type": "record",
  "name": "Record",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "type",
      "type": "string"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "ppu",
      "type": "double"
    },
    {
      "name": "batters.batter",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "namespace": "Record.batters",
          "name": "batter",
          "fields": [
            {
              "name": "id",
              "type": "string"
            },
            {
              "name": "type",
              "type": "string"
            }
          ]
        }
      }
    },
    {
      "name": "topping",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "namespace": "Record",
          "name": "topping",
          "fields": [
            {
              "name": "id",
              "type": "string"
            },
            {
              "name": "type",
              "type": "string"
            }
          ]
        }
      }
    }
  ]
}
