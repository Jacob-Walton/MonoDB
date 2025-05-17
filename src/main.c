/**
 * @file main.c
 * @brief Main entry point for the MonoDB database
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>  // For va_list, va_start, etc.

// NSQL Includes
#include <nsql/ast_serializer.h>
#include <nsql/ast_printer.h>
#include <nsql/error_reporter.h>
#include <nsql/lexer.h>
#include <nsql/parser.h>


#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>   // For CreateThread
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")  // Link with ws2_32.lib
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>  // For pthread_create
#include <sys/socket.h>
#include <unistd.h>  // For close()
#include <netinet/tcp.h> // For TCP_NODELAY
#include <errno.h>

#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close
// Define WSAGetLastError for non-Windows platforms if needed for compatibility
#ifndef _WIN32
#define WSAGetLastError() errno
#endif
#endif

#define PORT 5433
#define MAX_CONNECTIONS 5
#define BUFFER_SIZE 4096  // Increased buffer size
#define AST_BUFFER_SIZE 8192  // Buffer size for AST string representation

// Redirect printf output to a string buffer
typedef struct {
    char* buffer;
    size_t size;
    size_t capacity;
} StringBuffer;

// Initialize string buffer
void init_string_buffer(StringBuffer* sb, size_t initial_capacity) {
    sb->buffer = (char*)malloc(initial_capacity);
    if (sb->buffer == NULL) {
        fprintf(stderr, "Failed to allocate string buffer\n");
        exit(1);
    }
    sb->buffer[0] = '\0';
    sb->size = 0;
    sb->capacity = initial_capacity;
}

// Append to string buffer
void append_string_buffer(StringBuffer* sb, const char* format, ...) {
    va_list args;
    va_start(args, format);

    // Calculate required space
    va_list args_copy;
    va_copy(args_copy, args);
    int required = vsnprintf(NULL, 0, format, args_copy) + 1; // +1 for null terminator
    va_end(args_copy);

    // Ensure we have enough space
    if (sb->size + required > sb->capacity) {
        size_t new_capacity = sb->capacity * 2;
        if (new_capacity < sb->size + required)
            new_capacity = sb->size + required + 1024;

        char* new_buffer = (char*)realloc(sb->buffer, new_capacity);
        if (new_buffer == NULL) {
            fprintf(stderr, "Failed to reallocate string buffer\n");
            va_end(args);
            return;
        }

        sb->buffer = new_buffer;
        sb->capacity = new_capacity;
    }

    // Perform the actual append
    int written = vsnprintf(sb->buffer + sb->size, sb->capacity - sb->size, format, args);
    sb->size += written;

    va_end(args);
}

// Free string buffer
void free_string_buffer(StringBuffer* sb) {
    if (sb->buffer) {
        free(sb->buffer);
        sb->buffer = NULL;
    }
    sb->size = 0;
    sb->capacity = 0;
}

// Custom AST printing function that outputs to a string buffer
void print_ast_to_string(StringBuffer* sb, Node* node, int indent) {
    if (node == NULL) {
        for (int i = 0; i < indent; i++) {
            append_string_buffer(sb, "  ");
        }
        append_string_buffer(sb, "NULL\n");
        return;
    }

    for (int i = 0; i < indent; i++) {
        append_string_buffer(sb, "  ");
    }

    // Similar structure to print_ast but append to string buffer instead of printf
    switch (node->type) {
        case NODE_ASK_QUERY:
            append_string_buffer(sb, "ASK QUERY:\n");
            for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
            append_string_buffer(sb, "Source:\n");
            print_ast_to_string(sb, node->as.ask_query.source, indent + 2);

            for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
            append_string_buffer(sb, "Fields:\n");
            print_ast_to_string(sb, node->as.ask_query.fields, indent + 2);

            if (node->as.ask_query.condition != NULL) {
                for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
                append_string_buffer(sb, "Condition:\n");
                print_ast_to_string(sb, node->as.ask_query.condition, indent + 2);
            }

            if (node->as.ask_query.group_by != NULL) {
                for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
                append_string_buffer(sb, "Group By:\n");
                print_ast_to_string(sb, node->as.ask_query.group_by, indent + 2);
            }

            if (node->as.ask_query.order_by != NULL) {
                for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
                append_string_buffer(sb, "Order By:\n");
                print_ast_to_string(sb, node->as.ask_query.order_by, indent + 2);
            }

            if (node->as.ask_query.limit != NULL) {
                for (int i = 0; i < indent + 1; i++) append_string_buffer(sb, "  ");
                append_string_buffer(sb, "Limit:\n");
                print_ast_to_string(sb, node->as.ask_query.limit, indent + 2);
            }
            break;

        // Implement other node types similar to print_ast function
        // This is a simplified version - you'd need to implement all node types
        // from the original print_ast function

        default:
            append_string_buffer(sb, "NODE TYPE %d\n", node->type);
            break;
    }
}

// Function to handle a client connection
void handle_connection(SOCKET clientSocket) {
    char buffer[BUFFER_SIZE];
    int  recvResult;

    // Set TCP_NODELAY to reduce latency
#ifdef _WIN32
    BOOL flag = 1;
    setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(BOOL));
#else
    int flag = 1;
    setsockopt(clientSocket, IPPROTO_TCP, TCP_NODELAY, (void*)&flag, sizeof(int));
#endif

    printf("Client connected. Waiting for query...\n");

    // Receive until the peer shuts down the connection
    do {
        recvResult = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);
        if (recvResult > 0) {
            buffer[recvResult] = '\0';  // Null-terminate the received data
            printf("Received query (%d bytes)\n", recvResult);

            // --- NSQL Processing ---
            Lexer lexer;

            // Check if client wants JSON output
            bool json_mode = false;
            const char* query_input = buffer;

            if (strstr(buffer, "-- JSON_OUTPUT") != NULL) {
                json_mode = true;
                // Skip the JSON_OUTPUT directive
                query_input = strstr(buffer, "-- JSON_OUTPUT") + 14;
                while (*query_input && (*query_input == '\n' || *query_input == '\r' || *query_input == ' '))
                    query_input++;
            }

            // Initialize lexer with appropriate input
            lexer_init(&lexer, query_input);

            // Initialize parser
            Parser parser;
            parser_init(&parser, &lexer);

            // Parse the program
            Node* program = parse_program(&parser);

            // Initialize response buffer - use larger buffer for complex ASTs
            size_t response_size = AST_BUFFER_SIZE * 2; // Double buffer size for complex ASTs
            char* response_buffer = malloc(response_size);

            if (response_buffer == NULL) {
                // Memory allocation failed
                const char* error_msg = "Error: Server failed to allocate memory for response";
                send(clientSocket, error_msg, strlen(error_msg), 0);
                printf("ERROR: Failed to allocate response buffer\n");
                continue;
            }

            // Clear buffer to ensure proper initialization
            memset(response_buffer, 0, response_size);

            if (program && !parser.had_error) {
                // Query parsed successfully - use the AST printer
                AstPrinter printer;

                // Initialize printer with the requested format
                if (json_mode) {
                    ast_printer_init_buffer(&printer, AST_FORMAT_JSON, response_buffer, response_size);
                    printer.pretty_print = true;  // Enable pretty printing
                } else {
                    ast_printer_init_buffer(&printer, AST_FORMAT_TEXT, response_buffer, response_size);
                }

                // Add a header to the response
                const char* success_header = json_mode ?
                    "{\"status\":\"success\",\"message\":\"Query parsed successfully\",\"ast\":" :
                    "Query parsed successfully.\nAST Structure:\n\n";

                size_t header_len = strlen(success_header);
                memcpy(response_buffer, success_header, header_len);

                // Update printer state based on the format
                if (json_mode) {
                    printer.output.buf.buffer += header_len;
                    printer.output.buf.size -= header_len;
                } else {
                    printer.output.buf.written = header_len;
                }

                // Print the AST
                ast_printer_print(&printer, program);

                size_t total_written = json_mode ?
                    header_len + ast_printer_get_written(&printer) :
                    printer.output.buf.written;

                // Add JSON closing brace if needed
                if (json_mode) {
                    strcpy(response_buffer + total_written, "}");
                    total_written++;
                }

                // Free resources
                ast_printer_free(&printer);
                free_node(program);

                printf("Query parsed successfully. Response size: %zu bytes\n", total_written);

                // Send response in chunks if large
                size_t remaining = total_written;
                size_t offset = 0;
                while (remaining > 0) {
                    // Send at most 8KB at a time to avoid buffers filling up
                    size_t chunk_size = remaining > 8192 ? 8192 : remaining;
                    int sendResult = send(clientSocket, response_buffer + offset, chunk_size, 0);

                    if (sendResult == SOCKET_ERROR) {
                        printf("Send failed with error: %ld\n", WSAGetLastError());
                        break;
                    }

                    remaining -= sendResult;
                    offset += sendResult;
                }

                printf("Response sent completely (%zu bytes)\n", total_written);
            } else {
                // Query parsing failed - format errors
                char* error_buffer = response_buffer; // Reuse the buffer

                if (json_mode) {
                    // Format errors as JSON
                    strcpy(error_buffer, "{\"status\":\"error\",\"errors\":[");
                    size_t json_prefix_len = strlen(error_buffer);

                    size_t written = parser_format_errors_json(&parser,
                                                             error_buffer + json_prefix_len,
                                                             response_size - json_prefix_len - 2);

                    strcat(error_buffer, "]}");
                    written = strlen(error_buffer);
                } else {
                    // Format errors as text
                    size_t written = parser_format_errors(&parser, error_buffer, response_size);

                    if (written == 0) {
                        // Fallback if error formatting failed
#ifdef _MSC_VER
                        strcpy_s(error_buffer, response_size, "Error: Failed to parse query (no details available)");
#else
                        strcpy(error_buffer, "Error: Failed to parse query (no details available)");
#endif
                        written = strlen(error_buffer);
                    }
                }

                // Send the error response
                int sendResult = send(clientSocket, error_buffer, strlen(error_buffer), 0);
                if (sendResult == SOCKET_ERROR) {
                    printf("Send failed with error: %ld\n", WSAGetLastError());
                    free(response_buffer);
                    break;
                }
                printf("Error response sent (%d bytes)\n", sendResult);

                if (program) {
                    free_node(program);
                }
            }

            // Free the response buffer
            free(response_buffer);

        } else if (recvResult == 0) {
            printf("Client disconnected.\n");
        } else {
            printf("Recv failed with error: %ld\n", WSAGetLastError());
        }
    } while (recvResult > 0);

    // Cleanup for this client
    closesocket(clientSocket);
    printf("Connection closed.\n");
}

// Thread function to handle a single client connection
#ifdef _WIN32
DWORD WINAPI connection_thread_func(LPVOID lpParam) {
    SOCKET clientSocket = (SOCKET)lpParam;
    handle_connection(clientSocket);
    return 0;
}
#else
void* connection_thread_func(void* arg) {
    SOCKET* clientSocketPtr = (SOCKET*)arg;
    SOCKET  clientSocket    = *clientSocketPtr;
    free(clientSocketPtr);  // Free the allocated memory for the socket descriptor
    handle_connection(clientSocket);
    pthread_detach(pthread_self());  // Detach the thread so resources are freed automatically
    return NULL;
}
#endif

int main(int argc, char* argv[]) {
    (void)argc;  // Mark unused
    (void)argv;  // Mark unused

#ifdef _WIN32
    WSADATA wsaData;
    int     iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        printf("WSAStartup failed: %d\n", iResult);
        return 1;
    }
#endif

    SOCKET             listenSocket = INVALID_SOCKET;
    struct sockaddr_in serverAddr;

    printf("MonoDB - Starting up...\n");
    printf("MonoDB version 0.1.0\n");

    // Create socket
    listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket == INVALID_SOCKET) {
        printf("Error creating socket: %ld\n", WSAGetLastError());
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }

    // Prepare the sockaddr_in structure
    serverAddr.sin_family      = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port        = htons(PORT);

    // Bind
    if (bind(listenSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        printf("Bind failed with error: %ld\n", WSAGetLastError());
        closesocket(listenSocket);
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }

    // Listen
    if (listen(listenSocket, MAX_CONNECTIONS) == SOCKET_ERROR) {
        printf("Listen failed with error: %ld\n", WSAGetLastError());
        closesocket(listenSocket);
#ifdef _WIN32
        WSACleanup();
#endif
        return 1;
    }

    printf("MonoDB initialized successfully\n");
    printf("MonoDB listening on port %d\n", PORT);

    // --- Main Accept Loop ---
    while (1) {
        SOCKET             clientSocket = INVALID_SOCKET;
        struct sockaddr_in clientAddr;
        socklen_t clientAddrSize = sizeof(clientAddr);  // Use socklen_t for POSIX compatibility

        // Accept a client socket
        printf("Waiting for a connection...\n");
        clientSocket = accept(listenSocket, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSocket == INVALID_SOCKET) {
            printf("Accept failed with error: %ld\n", WSAGetLastError());
            // Decide if this is fatal. For now, continue listening.
            // Consider adding a delay or break condition on repeated errors.
            continue;
        }

        // Convert client IP to string for logging
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("Accepted connection from %s:%d\n", client_ip, ntohs(clientAddr.sin_port));

        // Handle the connection in a new thread
#ifdef _WIN32
        HANDLE hThread = CreateThread(NULL,                    // default security attributes
                                      0,                       // use default stack size
                                      connection_thread_func,  // thread function name
                                      (LPVOID)clientSocket,    // argument to thread function
                                      0,                       // use default creation flags
                                      NULL);                   // returns the thread identifier

        if (hThread == NULL) {
            printf("Error creating thread: %ld\n", GetLastError());
            closesocket(clientSocket);  // Close the socket if thread creation failed
        } else {
            CloseHandle(hThread);  // Close the handle as we don't need to join it
        }
#else
        pthread_t thread_id;
        SOCKET*   clientSocketPtr =
            malloc(sizeof(SOCKET));  // Allocate memory to pass socket descriptor
        if (!clientSocketPtr) {
            perror("Failed to allocate memory for client socket");
            closesocket(clientSocket);
            continue;  // Continue to next accept attempt
        }
        *clientSocketPtr = clientSocket;

        if (pthread_create(&thread_id, NULL, connection_thread_func, clientSocketPtr) != 0) {
            perror("Error creating thread");
            closesocket(clientSocket);
            free(clientSocketPtr);  // Free memory if thread creation failed
        }
        // Thread will detach itself, no need to join
#endif

        // The main loop continues to accept new connections immediately
        // handle_connection(clientSocket); // Original blocking call removed
    }
    // --- End Main Accept Loop ---

    /* Shutdown (currently unreachable in the loop) */
    printf("MonoDB shutting down...\n");

    // cleanup
    closesocket(listenSocket);  // Close listening socket when server exits
#ifdef _WIN32
    WSACleanup();
#endif

    return 0;
}
