#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Table/Row Specific
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

typedef struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

const uint32_t ID_OFFSET = 0;
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct Table_t {
    void* pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
} Table;

void serializeRow(Row* src, void* dest) {
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserializeRow(void* src, Row* dest) {
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

void* rowSlot(Table* table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
    void* page = table->pages[pageNum];

    if (!page) {
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }
    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;

    return page + byteOffset;
}

void printRow(Row* row) {
    fprintf(stdout, "%u\t%s\t%s\n", row->id, row->username, row->email);
}

Table* newTable() {
    Table* table = malloc(sizeof(Table));
    table->num_rows = 0;

    return table;
}

void freeTable(Table* table) {
    for (uint32_t i = 0; i < table->num_rows; ++i) {
        if (table->pages[i]) {
            free(table->pages[i]);
            table->pages[i] = NULL;
        }
    }
    table->num_rows = 0;
}
// End Table/Row Specific

// REPL/VM Specific
typedef enum MetaCommandResult_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
    META_COMMAND_EXIT
} MetaCommandResult;

typedef enum PrepareResult_t {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum ExecuteResult_t {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct InputBuffer_t {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct Statement_t {
    StatementType type;
    Row row_to_insert;
} Statement;

InputBuffer* newInputBuffer() {
    InputBuffer* inputBuffer = malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;

    return inputBuffer;
}

void freeInputBuffer(InputBuffer* inputBuffer) {
    if (inputBuffer->buffer) {
        free(inputBuffer->buffer);
    }

    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
        return META_COMMAND_EXIT;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement) {
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int argsAssigned = sscanf(inputBuffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
                                  statement->row_to_insert.username, statement->row_to_insert.email);
        if (argsAssigned < 3) {
            fprintf(stderr, "Syntax error\n");
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }

    if (strcmp(inputBuffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void readInput(InputBuffer* inputBuffer) {
    ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->buffer_length), stdin);

    if (bytesRead <= 0) {
        fprintf(stderr, "Error reading input\n");
    } else {
        inputBuffer->input_length = bytesRead - 1;
        inputBuffer->buffer[bytesRead - 1] = 0;
    }
}

void printPrompt() {
    printf("db > ");
}

ExecuteResult executeInsert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* toInsert = &(statement->row_to_insert);
    serializeRow(toInsert, rowSlot(table, table->num_rows));
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table) {
    Row row;

    for (uint32_t i = 0; i < table->num_rows; ++i) {
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }

    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return executeInsert(statement, table);
        case STATEMENT_SELECT:
            return executeSelect(statement, table);
    }
}

int main() {
    Table* table = newTable();
    InputBuffer* inputBuffer = newInputBuffer();
    bool quit = false;

    while (!quit) {
        printPrompt();
        readInput(inputBuffer);

        if (inputBuffer->buffer[0] == '.') {
            switch (doMetaCommand(inputBuffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    fprintf(stdout, "Unrecognized command '%s'.\n", inputBuffer->buffer);
                    continue;
                case META_COMMAND_EXIT:
                    quit = true;
                    continue;
            }
        }

        Statement statement;
        switch (prepareStatement(inputBuffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                fprintf(stdout, "Unrecognized command '%s'.\n", inputBuffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                fprintf(stdout, "Syntax error. Could not parse: '%s'.\n", inputBuffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case EXECUTE_SUCCESS:
                fprintf(stdout, "Executed!\n");
                break;
            case EXECUTE_TABLE_FULL:
                fprintf(stdout, "Error: Table full!\n");
                break;
        }
    }

    free(table);
    freeInputBuffer(inputBuffer);

    return EXIT_SUCCESS;
}