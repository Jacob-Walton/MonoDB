#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cctype>
#include <chrono>  // For time measurement functionality
#include <thread>  // For this_thread::sleep_for
#include <nsql/lexer.h>
#include <nsql/parser.h>
#include <nsql/ast_serializer.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h> // For close()
#include <cstring>
#include <cerrno>
#include <netinet/tcp.h> // For TCP_NODELAY
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close
#endif

#define SERVER_PORT 5433
#define SERVER_ADDR "127.0.0.1"
#define INITIAL_BUFFER_SIZE 16384  // Increased buffer size to handle larger responses

// REPL configuration
struct ReplConfig {
    bool color_output = true;       // Use ANSI colors in output
    bool json_mode = false;         // Request JSON output from server
    bool show_timing = true;        // Show query execution time
    bool verbose = false;           // Show detailed information
};

// ANSI color codes for terminal output
namespace Colors {
    const std::string reset = "\033[0m";
    const std::string bold = "\033[1m";
    const std::string red = "\033[31m";
    const std::string green = "\033[32m";
    const std::string yellow = "\033[33m";
    const std::string blue = "\033[34m";
    const std::string magenta = "\033[35m";
    const std::string cyan = "\033[36m";
    const std::string white = "\033[37m";
}

// Function to connect to the server
SOCKET connect_to_server() {
#ifdef _WIN32
    WSADATA wsaData;
    int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        std::cerr << "WSAStartup failed: " << iResult << std::endl;
        return INVALID_SOCKET;
    }
#endif

    SOCKET connectSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (connectSocket == INVALID_SOCKET) {
#ifdef _WIN32
        std::cerr << "Error creating socket: " << WSAGetLastError() << std::endl;
        WSACleanup();
#else
        std::cerr << "Error creating socket: " << strerror(errno) << std::endl;
#endif
        return INVALID_SOCKET;
    }

    // Set TCP_NODELAY to reduce latency
#ifdef _WIN32
    BOOL flag = 1;
    setsockopt(connectSocket, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(BOOL));
#else
    int flag = 1;
    setsockopt(connectSocket, IPPROTO_TCP, TCP_NODELAY, (void*)&flag, sizeof(int));
#endif

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    inet_pton(AF_INET, SERVER_ADDR, &serverAddr.sin_addr);
    serverAddr.sin_port = htons(SERVER_PORT);

    // Connect to server.
    if (connect(connectSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
#ifdef _WIN32
        std::cerr << "Unable to connect to server: " << WSAGetLastError() << std::endl;
        closesocket(connectSocket);
        WSACleanup();
#else
        std::cerr << "Unable to connect to server: " << strerror(errno) << std::endl;
        closesocket(connectSocket);
#endif
        return INVALID_SOCKET;
    }
    std::cout << "Connected to MonoDB server at " << SERVER_ADDR << ":" << SERVER_PORT << std::endl;
    return connectSocket;
}

// Function to receive the complete response from server
std::string receive_full_response(SOCKET socket) {
    // Increase buffer size for better performance with large AST responses
    const size_t chunk_size = 16384;
    std::string response;
    char* buffer = new char[chunk_size];
    int bytesReceived;
    
    // Set a timeout for recv to prevent hanging
#ifdef _WIN32
    int timeout = 5000; // 5 seconds in milliseconds
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
#else
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
#endif

    // Keep receiving data until we get a complete response
    do {
        bytesReceived = recv(socket, buffer, chunk_size - 1, 0);
        
        if (bytesReceived > 0) {
            buffer[bytesReceived] = '\0'; // Null-terminate
            response.append(buffer, bytesReceived);
            
            // Check if this might be a complete response
            if (bytesReceived < static_cast<int>(chunk_size) - 1) {
                // Likely got all data since we received less than the buffer size
                break;
            }
        } else if (bytesReceived == 0) {
            std::cout << "Server closed the connection." << std::endl;
            break;
        } else {
            // Only print error if it's not a timeout
#ifdef _WIN32
            if (WSAGetLastError() != WSAETIMEDOUT) {
                std::cerr << "Recv failed: " << WSAGetLastError() << std::endl;
            }
#else
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::cerr << "Recv failed: " << strerror(errno) << std::endl;
            }
#endif
            break;
        }
    } while (bytesReceived > 0);
    
    delete[] buffer;
    return response;
}

// Helper function to determine if a response contains an error message
bool is_error_response(const std::string& response) {
    return response.find("Error:") != std::string::npos || 
           response.find("NSQL Parsing Results:") != std::string::npos;
}

// Syntax highlight specific patterns in NSQL output
std::string syntax_highlight(const std::string& line, bool use_colors) {
    if (!use_colors) return line;
    
    std::string result = line;
    
    // Highlight node types (ASK QUERY, TELL QUERY, etc.)
    static const std::vector<std::string> node_types = {
        "ASK QUERY", "TELL QUERY", "FIND QUERY", "SHOW QUERY", "GET QUERY",
        "FIELD LIST", "SOURCE", "JOIN", "BINARY EXPRESSION", "IDENTIFIER",
        "STRING", "INTEGER", "DECIMAL", "GROUP BY", "ORDER BY", "LIMIT"
    };
    
    for (const auto& type : node_types) {
        std::size_t pos = 0;
        while ((pos = result.find(type, pos)) != std::string::npos) {
            result.insert(pos + type.length(), Colors::reset);
            result.insert(pos, Colors::cyan + Colors::bold);
            pos += type.length() + Colors::reset.length() + Colors::cyan.length() + Colors::bold.length();
        }
    }
    
    // Highlight properties (Source, Fields, Condition, etc.)
    static const std::vector<std::string> properties = {
        "Source:", "Fields:", "Condition:", "Group By:", "Order By:", "Limit:", 
        "Left:", "Right:", "Operator:"
    };
    
    for (const auto& prop : properties) {
        std::size_t pos = 0;
        while ((pos = result.find(prop, pos)) != std::string::npos) {
            result.insert(pos + prop.length(), Colors::reset);
            result.insert(pos, Colors::yellow);
            pos += prop.length() + Colors::reset.length() + Colors::yellow.length();
        }
    }
    
    // Highlight literals
    static const std::vector<std::pair<std::string, std::string>> literals = {
        {"STRING:", Colors::green},
        {"INTEGER:", Colors::magenta},
        {"DECIMAL:", Colors::magenta},
        {"IDENTIFIER:", Colors::blue}
    };
    
    for (const auto& [literal, color] : literals) {
        std::size_t pos = 0;
        while ((pos = result.find(literal, pos)) != std::string::npos) {
            // Find the end of this line
            std::size_t eol = result.find('\n', pos);
            if (eol == std::string::npos) eol = result.length();
            
            // Insert colors
            result.insert(eol, Colors::reset);
            result.insert(pos + literal.length(), color);
            
            pos = eol + Colors::reset.length();
        }
    }
    
    return result;
}

// Format the output with syntax highlighting
void display_response(const std::string& response, const ReplConfig& config) {
    // Different header style based on whether it's an error or success
    bool is_error = is_error_response(response);
    
    std::string header = config.color_output 
        ? (is_error ? Colors::red + Colors::bold : Colors::green + Colors::bold) 
        : "";
    
    std::string header_text = is_error ? "ERROR RESPONSE" : "SERVER RESPONSE";
    std::string border = std::string(60, '=');
    
    std::cout << header << border << Colors::reset << std::endl;
    std::cout << header << std::setw(30) << std::setfill(' ') << header_text << Colors::reset << std::endl;
    std::cout << header << border << Colors::reset << std::endl;
    
    // Process the response line by line for syntax highlighting
    std::istringstream stream(response);
    std::string line;
    
    while (std::getline(stream, line)) {
        // Apply syntax highlighting if enabled and it's not an error
        std::string formatted_line = (config.color_output && !is_error) 
            ? syntax_highlight(line, config.color_output) 
            : line;
        
        std::cout << formatted_line << std::endl;
    }
    
    std::cout << header << border << Colors::reset << std::endl;
}

// Display REPL help information
void show_help() {
    std::cout << "\n---- MonoDB REPL Help ----\n"
              << "Available commands:\n"
              << "  .help               Display this help information\n"
              << "  .exit, .quit        Exit the REPL\n"
              << "  .clear              Clear the screen\n"
              << "  .connect [host:port] Attempt to reconnect to server\n" 
              << "  .mode [text|json]   Set output mode (text or JSON)\n"
              << "  .color [on|off]     Toggle color output\n"
              << "  .verbose [on|off]   Toggle verbose mode\n"
              << "  .timing [on|off]    Toggle query timing information\n"
              << "\n"
              << "NSQL queries can be entered directly. End with ';' or 'PLEASE' to execute.\n"
              << "Use empty line during multi-line input to cancel.\n"
              << std::endl;
}

// Clear the terminal screen
void clear_screen() {
#ifdef _WIN32
    system("cls");
#else
    system("clear");
#endif
}

// Process REPL commands (lines starting with '.')
bool process_command(const std::string& cmd, bool& running, ReplConfig& config, SOCKET& serverSocket) {
    std::string command = cmd.substr(1); // Remove the dot
    std::transform(command.begin(), command.end(), command.begin(), 
                  [](unsigned char c){ return static_cast<char>(std::tolower(c)); }); // Add explicit cast
    
    if (command == "exit" || command == "quit") {
        running = false;
        return true;
    }
    else if (command == "help") {
        show_help();
        return true;
    }
    else if (command == "clear") {
        clear_screen();
        return true;
    }
    else if (command.compare(0, 4, "mode") == 0) {
        if (command.find("json") != std::string::npos) {
            config.json_mode = true;
            std::cout << "Output mode set to JSON" << std::endl;
        } else {
            config.json_mode = false;
            std::cout << "Output mode set to text" << std::endl;
        }
        return true;
    }
    else if (command.compare(0, 5, "color") == 0) {
        if (command.find("off") != std::string::npos) {
            config.color_output = false;
            std::cout << "Color output disabled" << std::endl;
        } else {
            config.color_output = true;
            std::cout << "Color output enabled" << std::endl;
        }
        return true;
    }
    else if (command.compare(0, 7, "verbose") == 0) {
        if (command.find("off") != std::string::npos) {
            config.verbose = false;
            std::cout << "Verbose mode disabled" << std::endl;
        } else {
            config.verbose = true;
            std::cout << "Verbose mode enabled" << std::endl;
        }
        return true;
    }
    else if (command.compare(0, 6, "timing") == 0) {
        if (command.find("off") != std::string::npos) {
            config.show_timing = false;
            std::cout << "Query timing disabled" << std::endl;
        } else {
            config.show_timing = true;
            std::cout << "Query timing enabled" << std::endl;
        }
        return true;
    }
    else if (command.compare(0, 7, "connect") == 0) {
        // Close existing socket if open
        if (serverSocket != INVALID_SOCKET) {
            closesocket(serverSocket);
        }
        
        // Connect to server
        serverSocket = connect_to_server();
        if (serverSocket == INVALID_SOCKET) {
            std::cerr << "Failed to connect to server" << std::endl;
        }
        return true;
    }
    
    std::cout << "Unknown command: ." << command << std::endl;
    return true;
}

int main() {
    SOCKET serverSocket = connect_to_server();
    if (serverSocket == INVALID_SOCKET) {
        return 1;
    }

    std::cout << "NSQL REPL connected to MonoDB. Type '.help' for commands or '.exit' to quit.\n";

    std::string buffer;
    std::string line;
    bool continuing = false;
    bool running = true;
    ReplConfig config;

    while (running) {
        // Different prompts for initial and continuation lines
        if (continuing) {
            std::cout << "... > ";
        } else {
            std::cout << (config.color_output ? Colors::cyan : "") << "nsql> " 
                      << (config.color_output ? Colors::reset : "");
        }

        if (!std::getline(std::cin, line)) break;

        // Process commands (lines starting with '.')
        if (!continuing && !line.empty() && line[0] == '.') {
            if (process_command(line, running, config, serverSocket)) {
                continue;
            }
        }
        
        // Check for exit commands
        if (!continuing && (line == "exit" || line == "quit")) break;
        if (line.empty()) {
            if (continuing) {
                // Empty line during multiline input - treat as cancel
                continuing = false;
                buffer.clear();
                std::cout << "Query input canceled.\n";
            }
            continue;
        }

        // Add this line to our buffer
        if (!buffer.empty()) {
            buffer += " ";  // Space between lines
        }
        buffer += line;

        // Check if the statement is complete (ends with PLEASE or ;)
        bool isComplete = false;
        if (line.find("PLEASE") != std::string::npos || line.find(';') != std::string::npos) {
            continuing = false;
            isComplete = true;
        } else {
            continuing = true;
        }

        // Process complete statements
        if (isComplete) {
            if (config.verbose) {
                std::cout << "Sending query: " << buffer << std::endl;
            }
            
            // Add format hint if in JSON mode
            std::string query_to_send = buffer;
            if (config.json_mode) {
                query_to_send = "-- JSON_OUTPUT\n" + query_to_send;
            }
            
            // Record start time if timing is enabled
            auto start_time = std::chrono::high_resolution_clock::now();
            
            // Send the query to the server in chunks if large
            const int max_chunk_size = 8192;
            int remaining = static_cast<int>(query_to_send.length());
            int offset = 0;
            
            while (remaining > 0) {
                int chunk_size = (remaining > max_chunk_size) ? max_chunk_size : remaining;
                int sendResult = send(serverSocket, query_to_send.c_str() + offset, chunk_size, 0);
                
                if (sendResult == SOCKET_ERROR) {
#ifdef _WIN32
                    std::cerr << "Send failed: " << WSAGetLastError() << std::endl;
#else
                    std::cerr << "Send failed: " << strerror(errno) << std::endl;
#endif
                    break;
                }
                
                remaining -= sendResult;
                offset += sendResult;
            }
            
            if (remaining > 0) {
                std::cerr << "Failed to send complete query." << std::endl;
                break;
            }
            
            // Wait for a moment to ensure server processes the query
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            // Receive and display the response from the server
            std::string response = receive_full_response(serverSocket);
            
            // Calculate elapsed time
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            if (!response.empty()) {
                display_response(response, config);
                
                // Show timing information if enabled
                if (config.show_timing) {
                    std::cout << "Query time: " << duration.count() << " ms" << std::endl;
                }
            } else {
                std::cerr << "Failed to receive response from server." << std::endl;
                break;
            }

            // Clear buffer for next query
            buffer.clear();
        }
    }

    // Cleanup
    closesocket(serverSocket);
#ifdef _WIN32
    WSACleanup();
#endif

    return 0;
}