#include <nsql/processor.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Forward declarations for the executor functions
static bool execute_ask_query(Node* node);
static bool execute_tell_query(Node* node);
static bool execute_find_query(Node* node);
static bool execute_show_query(Node* node);
static bool execute_get_query(Node* node);

bool nsql_processor_init(void) {
    printf("Initializing NSQL query processor\n");
    return true;
}

bool nsql_process_query(const char* query) {
    if (!query || *query == '\0') {
        return false;
    }
    
    // Initialize lexer with query string
    Lexer lexer;
    lexer_init(&lexer, query);
    
    // Initialize parser with lexer
    Parser parser;
    parser_init(&parser, &lexer);
    
    // Parse the query
    Node* ast = parse_query(&parser);
    if (!ast) {
        fprintf(stderr, "Error parsing query: %s\n", query);
        return false;
    }
    
    bool result = false;
    
    // Dispatch to the appropriate executor based on query type
    switch (ast->type) {
        case NODE_ASK_QUERY:
            result = execute_ask_query(ast);
            break;
        case NODE_TELL_QUERY:
            result = execute_tell_query(ast);
            break;
        case NODE_FIND_QUERY:
            result = execute_find_query(ast);
            break;
        case NODE_SHOW_QUERY:
            result = execute_show_query(ast);
            break;
        case NODE_GET_QUERY:
            result = execute_get_query(ast);
            break;
        default:
            fprintf(stderr, "Unsupported query type\n");
            result = false;
    }
    
    // Free the AST
    free_node(ast);
    
    return result;
}

void nsql_processor_shutdown(void) {
    printf("Shutting down NSQL query processor\n");
}

// Query executor implementation functions
static bool execute_ask_query(Node* node) {
    (void)node; // Mark as intentionally unused
    printf("Executing ASK query\n");
    // TODO: Implement query execution
    return true;
}

static bool execute_tell_query(Node* node) {
    (void)node; // Mark as intentionally unused
    printf("Executing TELL query\n");
    // TODO: Implement query execution
    return true;
}

static bool execute_find_query(Node* node) {
    (void)node; // Mark as intentionally unused
    printf("Executing FIND query\n");
    // TODO: Implement query execution
    return true;
}

static bool execute_show_query(Node* node) {
    (void)node; // Mark as intentionally unused
    printf("Executing SHOW query\n");
    // TODO: Implement query execution
    return true;
}

static bool execute_get_query(Node* node) {
    (void)node; // Mark as intentionally unused
    printf("Executing GET query\n");
    // TODO: Implement query execution
    return true;
}