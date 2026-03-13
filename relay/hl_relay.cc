// hl_relay.cc — Single-threaded async WebSocket relay for Hyperliquid node data.
//
// Tails hl-node data files via inotify, streams raw JSON lines to WebSocket
// clients. Supports per-coin filtering and channel selection.
//
// Build: see relay/build.sh  (requires spdlog headers in relay/spdlog/)
// Usage: ./relay/hl_relay --ws-port 8766 --data-dir ~/hl/data

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>

#include <sys/inotify.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/ansicolor_sink.h>
#include <spdlog/sinks/basic_file_sink.h>

#include <algorithm>
#include <chrono>
#include <cstdarg>
#include <cstring>
#include <ctime>
#include <deque>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
using tcp = net::ip::tcp;

// ============================================================================
// Logging — printf-style wrappers over spdlog
// ============================================================================

// Forward declaration so LOG_* macros work before setup_logger() is called.
static void log_msg(spdlog::level::level_enum level, const char* fmt, ...)
    __attribute__((format(printf, 2, 3)));
static void log_msg(spdlog::level::level_enum level, const char* fmt, ...) {
    char buf[2048];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    spdlog::log(level, buf);
}

#define LOG_D(...) log_msg(spdlog::level::debug, __VA_ARGS__)
#define LOG_I(...) log_msg(spdlog::level::info,  __VA_ARGS__)
#define LOG_W(...) log_msg(spdlog::level::warn,  __VA_ARGS__)
#define LOG_E(...) log_msg(spdlog::level::err,   __VA_ARGS__)

static void setup_logger(bool verbose, const std::string& log_file) {
    std::vector<spdlog::sink_ptr> sinks;

    auto stderr_sink = std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>();
    stderr_sink->set_pattern("[%Y-%m-%dT%H:%M:%S.%e] [%^%l%$] %v");
    sinks.push_back(stderr_sink);

    if (!log_file.empty()) {
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, /*truncate=*/false);
        file_sink->set_pattern("[%Y-%m-%dT%H:%M:%S.%e] [%l] %v");
        sinks.push_back(file_sink);
    }

    auto logger = std::make_shared<spdlog::logger>("hl_relay", sinks.begin(), sinks.end());
    logger->set_level(verbose ? spdlog::level::debug : spdlog::level::info);
    logger->flush_on(spdlog::level::warn);
    spdlog::set_default_logger(logger);
}

// ============================================================================
// FileTailer — POSIX I/O based file tailing
// ============================================================================

struct FileTailer {
    int fd = -1;
    off_t file_pos = 0;
    std::string path;
    std::string line_buffer;
    static constexpr size_t READ_BUF_SIZE = 65536;
    char read_buf[READ_BUF_SIZE];

    void open_at_end(const std::string& p) {
        close();
        path = p;
        fd = ::open(p.c_str(), O_RDONLY);
        if (fd >= 0)
            file_pos = ::lseek(fd, 0, SEEK_END);
        line_buffer.clear();
    }

    void open_at_start(const std::string& p) {
        close();
        path = p;
        fd = ::open(p.c_str(), O_RDONLY);
        file_pos = 0;
        line_buffer.clear();
    }

    void reopen(const std::string& p) {
        open_at_end(p);
    }

    void close() {
        if (fd >= 0) { ::close(fd); fd = -1; }
        line_buffer.clear();
    }

    // Read new complete lines. Incomplete trailing data is buffered.
    std::vector<std::string> read_new_lines() {
        std::vector<std::string> lines;
        if (fd < 0) return lines;

        ::lseek(fd, file_pos, SEEK_SET);
        while (true) {
            ssize_t n = ::read(fd, read_buf, READ_BUF_SIZE);
            if (n <= 0) break;
            line_buffer.append(read_buf, static_cast<size_t>(n));
            file_pos += n;
        }

        size_t pos = 0;
        while (true) {
            size_t nl = line_buffer.find('\n', pos);
            if (nl == std::string::npos) break;
            if (nl > pos)
                lines.emplace_back(line_buffer, pos, nl - pos);
            pos = nl + 1;
        }
        if (pos > 0) line_buffer.erase(0, pos);
        return lines;
    }

    ~FileTailer() { close(); }
};

// ============================================================================
// Path helpers — UTC time-based paths
// ============================================================================

static std::string get_hourly_base(const std::string& base_dir) {
    return base_dir + "/hourly";
}

static std::string get_current_date_dir(const std::string& base_dir) {
    time_t now = time(nullptr);
    struct tm tm;
    gmtime_r(&now, &tm);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d%02d%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return base_dir + "/hourly/" + buf;
}

static std::string get_current_hour_path(const std::string& base_dir) {
    time_t now = time(nullptr);
    struct tm tm;
    gmtime_r(&now, &tm);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d%02d%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return base_dir + "/hourly/" + buf + "/" + std::to_string(tm.tm_hour);
}

// ============================================================================
// Coin filtering — raw string splice (no DOM parse, no re-serialize)
// ============================================================================

// Scan forward from pos, skipping one JSON value (object, array, string,
// number, bool, or null). Assumes compact JSON. Returns position after value.
static size_t skip_json_value(const char* data, size_t len, size_t pos) {
    if (pos >= len) return len;
    char ch = data[pos];
    if (ch == '{' || ch == '[') {
        char close = (ch == '{') ? '}' : ']';
        int depth = 1;
        ++pos;
        bool in_str = false;
        while (pos < len && depth > 0) {
            char c = data[pos];
            if (in_str) {
                if (c == '\\') { pos = std::min(len, pos + size_t{2}); continue; }
                if (c == '"') in_str = false;
            } else {
                if (c == '"') in_str = true;
                else if (c == ch) ++depth;
                else if (c == close) --depth;
            }
            ++pos;
        }
        return pos;
    }
    if (ch == '"') {
        ++pos;
        while (pos < len) {
            if (data[pos] == '\\') { pos = std::min(len, pos + size_t{2}); continue; }
            if (data[pos] == '"') return pos + 1;
            ++pos;
        }
        return pos;
    }
    // number, bool, null — scan until delimiter
    while (pos < len && data[pos] != ',' && data[pos] != '}' && data[pos] != ']')
        ++pos;
    return pos;
}

// Check if any coin pattern matches within [start, end) of data
static bool coin_matches(const char* data, size_t start, size_t end,
                          const std::vector<std::pair<std::string, size_t>>& patterns) {
    if (start >= end) return false;
    std::string_view view(data + start, end - start);
    for (auto& [pat, pat_len] : patterns) {
        if (view.find(std::string_view(pat.data(), pat_len)) != std::string_view::npos)
            return true;
    }
    return false;
}

static bool contains_any_quoted_coin(const std::string& raw,
                                     const std::unordered_set<std::string>& coins) {
    for (const auto& c : coins) {
        std::string quoted;
        quoted.reserve(c.size() + 2);
        quoted.push_back('"');
        quoted.append(c);
        quoted.push_back('"');
        if (raw.find(quoted) != std::string::npos)
            return true;
    }
    return false;
}

// Build coin patterns for substring matching (both compact and spaced forms).
static std::vector<std::pair<std::string, size_t>>
build_coin_patterns(const std::unordered_set<std::string>& coins) {
    std::vector<std::pair<std::string, size_t>> patterns;
    patterns.reserve(coins.size() * 2);
    for (auto& c : coins) {
        std::string p1 = "\"coin\":\"" + c + "\"";
        std::string p2 = "\"coin\": \"" + c + "\"";
        patterns.push_back({p1, p1.size()});
        patterns.push_back({p2, p2.size()});
    }
    return patterns;
}

// Filter a book-diffs JSON line to only include events for given coins.
// Handles both batch-by-block (B) and line/single-event (L) formats.
// Uses raw string splicing — no DOM parse, no re-serialize.
static std::string filter_diffs_line(const std::string& raw,
                                     const std::unordered_set<std::string>& coins) {
    const char* data = raw.data();
    const size_t len = raw.size();

    // Fast string pre-check
    if (!contains_any_quoted_coin(raw, coins)) return {};

    auto patterns = build_coin_patterns(coins);

    // Line format: single event {"coin":"BTC",...} — no block wrapper
    if (raw.find("\"block_number\"") == std::string::npos) {
        return coin_matches(data, 0, len, patterns) ? raw : std::string{};
    }

    // Batch-by-block format: find "events" array and splice matching events
    const char* ekey = "\"events\"";
    const char* found = strstr(data, ekey);
    if (!found) return {};
    size_t pos_after_key = (found - data) + strlen(ekey);
    // Skip optional whitespace and colon
    while (pos_after_key < len && (data[pos_after_key] == ' ' ||
           data[pos_after_key] == ':' || data[pos_after_key] == '\t')) ++pos_after_key;
    if (pos_after_key >= len || data[pos_after_key] != '[') return {};
    size_t arr_start = pos_after_key; // points at '['

    std::string result;
    result.reserve(raw.size());
    result.append(data, arr_start + 1); // everything up to and including '['

    size_t pos = arr_start + 1;
    bool first_match = true;
    int match_count = 0;

    while (pos < len) {
        while (pos < len && (data[pos] == ',' || data[pos] == ' ')) ++pos;
        if (pos >= len || data[pos] == ']') break;

        size_t ev_start = pos;
        size_t ev_end = skip_json_value(data, len, pos);

        if (coin_matches(data, ev_start, ev_end, patterns)) {
            if (!first_match) result.push_back(',');
            result.append(data + ev_start, ev_end - ev_start);
            first_match = false;
            ++match_count;
        }

        pos = ev_end;
    }

    if (match_count == 0) return {};

    result.push_back(']');
    // Skip past original ']'
    while (pos < len && data[pos] != ']') ++pos;
    if (pos < len) ++pos;
    result.append(data + pos, len - pos);
    return result;
}

// Filter a fills JSON line. In batch format fill events come in pairs;
// check coin in the first element and keep both together.
// Handles both batch-by-block (B) and line/single-event (L) formats.
static std::string filter_fills_line(const std::string& raw,
                                     const std::unordered_set<std::string>& coins) {
    const char* data = raw.data();
    const size_t len = raw.size();

    if (!contains_any_quoted_coin(raw, coins)) return {};

    auto patterns = build_coin_patterns(coins);

    // Line format: single fill event ["addr", fill_obj] — no block wrapper
    if (raw.find("\"block_number\"") == std::string::npos) {
        return coin_matches(data, 0, len, patterns) ? raw : std::string{};
    }

    // Batch-by-block format: find "events" array, process pairs
    const char* ekey = "\"events\"";
    const char* found = strstr(data, ekey);
    if (!found) return {};
    size_t pos_after_key = (found - data) + strlen(ekey);
    while (pos_after_key < len && (data[pos_after_key] == ' ' ||
           data[pos_after_key] == ':' || data[pos_after_key] == '\t')) ++pos_after_key;
    if (pos_after_key >= len || data[pos_after_key] != '[') return {};
    size_t arr_start = pos_after_key; // points at '['

    std::string result;
    result.reserve(raw.size());
    result.append(data, arr_start + 1);

    size_t pos = arr_start + 1;
    bool first_match = true;
    int match_count = 0;

    while (pos < len) {
        while (pos < len && (data[pos] == ',' || data[pos] == ' ')) ++pos;
        if (pos >= len || data[pos] == ']') break;

        // Element A of pair
        size_t a_start = pos;
        size_t a_end = skip_json_value(data, len, pos);
        pos = a_end;

        // Skip comma between pair elements
        while (pos < len && (data[pos] == ',' || data[pos] == ' ')) ++pos;
        if (pos >= len || data[pos] == ']') break;

        // Element B of pair
        size_t b_start = pos;
        size_t b_end = skip_json_value(data, len, pos);
        pos = b_end;

        // Check coin in element A
        if (coin_matches(data, a_start, a_end, patterns)) {
            if (!first_match) result.push_back(',');
            result.append(data + a_start, a_end - a_start);
            result.push_back(',');
            result.append(data + b_start, b_end - b_start);
            first_match = false;
            ++match_count;
        }
    }

    if (match_count == 0) return {};

    result.push_back(']');
    while (pos < len && data[pos] != ']') ++pos;
    if (pos < len) ++pos;
    result.append(data + pos, len - pos);
    return result;
}

// ============================================================================
// Channel map
// ============================================================================

static const std::unordered_map<std::string, char> CHANNEL_MAP = {
    {"book", 'D'}, {"trade", 'F'}
};

// ============================================================================
// Client state
// ============================================================================

class Relay;  // forward

struct Client : public std::enable_shared_from_this<Client> {
    websocket::stream<beast::tcp_stream> ws;
    std::string addr;
    std::unordered_set<std::string> coins;  // empty = all (unfiltered)
    bool has_coin_filter = false;
    std::string coin_key;                   // sorted coins joined by ',' for dedup
    std::unordered_set<char> channels;      // 'D' and/or 'F'
    std::deque<std::shared_ptr<const std::string>> write_queue;
    int consecutive_drops = 0;
    bool writing = false;
    bool active = true;
    net::steady_timer deadline;

    static constexpr size_t MAX_QUEUE = 2000;
    static constexpr int MAX_CONSECUTIVE_DROPS = 500;

    Client(tcp::socket&& sock)
        : ws(std::move(sock))
        , deadline(ws.get_executor())
    {
        channels.insert('D');
        channels.insert('F');
    }

    void enqueue(std::shared_ptr<const std::string> msg) {
        if (!active) return;
        if (write_queue.size() >= MAX_QUEUE) {
            ++consecutive_drops;
            if (consecutive_drops > MAX_CONSECUTIVE_DROPS) {
                LOG_W("Disconnecting slow consumer: %s (>%d consecutive drops)",
                      addr.c_str(), MAX_CONSECUTIVE_DROPS);
                active = false;
                do_close();
            }
            return;
        }
        consecutive_drops = 0;
        write_queue.push_back(std::move(msg));
        kick_writer();
    }

    // Cap coalesced WebSocket message size. Beast clients default to
    // read_message_max = 16 MB; stay well under to avoid blowing up
    // receivers and to keep latency bounded.
    static constexpr size_t MAX_BATCH_BYTES = 4 * 1024 * 1024;  // 4 MB

    void kick_writer() {
        if (writing || write_queue.empty() || !active) return;
        writing = true;

        // Coalesce queued messages into one buffer up to MAX_BATCH_BYTES.
        // Clients parse by newline so batching is transparent.
        auto batch = std::make_shared<std::string>();
        size_t total = 0;
        for (auto& m : write_queue) {
            if (total > 0 && total + m->size() > MAX_BATCH_BYTES)
                break;
            total += m->size();
        }
        batch->reserve(total);
        size_t consumed = 0;
        while (!write_queue.empty()) {
            auto& m = write_queue.front();
            if (consumed > 0 && consumed + m->size() > MAX_BATCH_BYTES)
                break;
            consumed += m->size();
            batch->append(*m);
            write_queue.pop_front();
        }

        auto self = shared_from_this();
        ws.async_write(
            net::buffer(*batch),
            [self, batch](beast::error_code ec, std::size_t) {
                self->writing = false;
                if (ec) {
                    if (ec != websocket::error::closed &&
                        ec != net::error::operation_aborted)
                        LOG_D("Write error for %s: %s", self->addr.c_str(), ec.message().c_str());
                    self->active = false;
                    return;
                }
                // Drain any messages that arrived while writing
                self->kick_writer();
            });
    }

    void do_close() {
        beast::error_code ec;
        ws.async_close(websocket::close_code::going_away,
            [self = shared_from_this()](beast::error_code) {});
    }
};

// ============================================================================
// Dispatcher
// ============================================================================

class Dispatcher {
public:
    uint64_t seq = 0;
    std::vector<std::shared_ptr<Client>> clients;

    uint64_t next_seq() { return ++seq; }

    void dispatch(char channel, const std::string& raw_line) {
        uint64_t s = next_seq();
        char fmt = (raw_line.find("\"block_number\"") != std::string::npos) ? 'B' : 'L';

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        int64_t relay_ns = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

        // Build prefix into stack buffer
        char prefix_buf[80];
        int prefix_len = snprintf(prefix_buf, sizeof(prefix_buf),
                                  "%c%c %lu %ld ", channel, fmt, s, relay_ns);

        // Pre-build unfiltered message
        auto unfiltered = std::make_shared<std::string>();
        unfiltered->reserve(prefix_len + raw_line.size() + 1);
        unfiltered->append(prefix_buf, prefix_len);
        unfiltered->append(raw_line);
        unfiltered->push_back('\n');

        // Remove inactive clients
        clients.erase(
            std::remove_if(clients.begin(), clients.end(),
                [](auto& c) { return !c->active; }),
            clients.end());

        // Dedup cache: coin_key -> filtered message (shared across clients
        // with the same coin set, computed at most once per dispatch)
        filter_cache_.clear();

        for (auto& client : clients) {
            if (client->channels.count(channel) == 0) continue;

            if (!client->has_coin_filter) {
                // Zero-copy: share the same string
                client->enqueue(unfiltered);
            } else {
                // Check dedup cache first
                auto cache_it = filter_cache_.find(client->coin_key);
                if (cache_it != filter_cache_.end()) {
                    // Cache hit — reuse previously computed result
                    if (cache_it->second)
                        client->enqueue(cache_it->second);
                    continue;
                }

                // Cache miss — compute filter
                std::string filtered;
                if (channel == 'D')
                    filtered = filter_diffs_line(raw_line, client->coins);
                else
                    filtered = filter_fills_line(raw_line, client->coins);

                if (filtered.empty()) {
                    // No match — cache nullptr so we skip for other
                    // clients with same coin set
                    filter_cache_[client->coin_key] = nullptr;
                    continue;
                }

                auto msg = std::make_shared<std::string>();
                msg->reserve(prefix_len + filtered.size() + 1);
                msg->append(prefix_buf, prefix_len);
                msg->append(filtered);
                msg->push_back('\n');
                filter_cache_[client->coin_key] = msg;
                client->enqueue(std::move(msg));
            }
        }
    }

    void add_client(std::shared_ptr<Client> c) {
        clients.push_back(std::move(c));
    }

    void remove_client(const std::shared_ptr<Client>& c) {
        c->active = false;
        clients.erase(
            std::remove_if(clients.begin(), clients.end(),
                [&c](auto& x) { return x.get() == c.get(); }),
            clients.end());
    }

private:
    // Per-dispatch dedup cache: coin_key -> filtered message (or nullptr if no match)
    std::unordered_map<std::string, std::shared_ptr<const std::string>> filter_cache_;
};

// ============================================================================
// InotifyWatcher
// ============================================================================

class InotifyWatcher {
public:
    InotifyWatcher(net::io_context& ioc, Dispatcher& dispatcher,
                   const std::string& data_dir, bool line_format = false)
        : ioc_(ioc)
        , dispatcher_(dispatcher)
        , data_dir_(data_dir)
        , book_base_(data_dir + (line_format ? "/node_raw_book_diffs" : "/node_raw_book_diffs_by_block"))
        , fills_base_(data_dir + (line_format ? "/node_fills" : "/node_fills_by_block"))
        , rotation_timer_(ioc)
        , inotify_sd_(ioc)
    {
        ifd_ = inotify_init1(IN_NONBLOCK);
        if (ifd_ < 0)
            throw std::runtime_error("Failed to initialize inotify");
        inotify_sd_.assign(ifd_);
    }

    void start() {
        setup_watches();
        start_inotify_read();
        start_rotation_timer();
    }

    void stop() {
        rotation_timer_.cancel();
        inotify_sd_.cancel();
        for (auto& [key, tailer] : tailers_)
            tailer.close();
        // inotify fd closed when stream_descriptor is destroyed
    }

private:
    net::io_context& ioc_;
    Dispatcher& dispatcher_;
    std::string data_dir_;
    std::string book_base_;
    std::string fills_base_;
    net::steady_timer rotation_timer_;
    int ifd_ = -1;
    net::posix::stream_descriptor inotify_sd_;

    // wd -> (source_type, path)
    std::unordered_map<int, std::pair<std::string, std::string>> watch_map_;
    std::unordered_map<std::string, FileTailer> tailers_;  // "book" / "fills"

    static constexpr size_t INOTIFY_BUF_SIZE = 65536;
    char inotify_buf_[INOTIFY_BUF_SIZE];

    int add_watch(const std::string& path, const std::string& source_type, uint32_t mask) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) return -1;
        int wd = inotify_add_watch(ifd_, path.c_str(), mask);
        if (wd >= 0) {
            watch_map_[wd] = {source_type, path};
            LOG_D("Added watch: wd=%d type=%s path=%s", wd, source_type.c_str(), path.c_str());
        } else {
            LOG_W("Failed to add watch: type=%s path=%s", source_type.c_str(), path.c_str());
        }
        return wd;
    }

    void remove_watch(int wd) {
        inotify_rm_watch(ifd_, wd);
        watch_map_.erase(wd);
    }

    void setup_watches() {
        struct { std::string base; std::string src_type; } sources[] = {
            {book_base_, "book"},
            {fills_base_, "fills"},
        };

        for (auto& [base, src_type] : sources) {
            // Watch hourly base dir for new date dirs (midnight rotation)
            std::string hourly_base = get_hourly_base(base);
            // Ensure directory exists
            mkdir(hourly_base.c_str(), 0755);
            add_watch(hourly_base, src_type + "_hourly_base", IN_CREATE);

            // Watch current date dir for new hour files
            std::string date_dir = get_current_date_dir(base);
            mkdir(date_dir.c_str(), 0755);
            add_watch(date_dir, src_type + "_date_dir", IN_CREATE);

            // Set up tailer for current hour file
            std::string hour_path = get_current_hour_path(base);
            struct stat st;
            if (stat(hour_path.c_str(), &st) == 0) {
                add_watch(hour_path, src_type, IN_MODIFY);
                tailers_[src_type].open_at_end(hour_path);
                LOG_I("Tailing %s: %s", src_type.c_str(), hour_path.c_str());
            } else {
                LOG_I("Waiting for %s file: %s", src_type.c_str(), hour_path.c_str());
            }
        }
    }

    void start_inotify_read() {
        inotify_sd_.async_read_some(
            net::buffer(inotify_buf_, INOTIFY_BUF_SIZE),
            [this](beast::error_code ec, std::size_t bytes_read) {
                if (ec) {
                    if (ec != net::error::operation_aborted)
                        LOG_E("inotify read error: %s", ec.message().c_str());
                    return;
                }
                process_inotify_events(bytes_read);
                // Poll all tailers after processing events
                read_and_dispatch("book");
                read_and_dispatch("fills");
                start_inotify_read();
            });
    }

    void process_inotify_events(std::size_t bytes_read) {
        size_t offset = 0;
        while (offset + sizeof(struct inotify_event) <= bytes_read) {
            auto* event = reinterpret_cast<struct inotify_event*>(inotify_buf_ + offset);
            size_t event_size = sizeof(struct inotify_event) + event->len;
            if (offset + event_size > bytes_read) break;

            int wd = event->wd;
            auto it = watch_map_.find(wd);
            if (it == watch_map_.end()) {
                offset += event_size;
                continue;
            }

            auto& [src_type, path] = it->second;
            std::string name;
            if (event->len > 0)
                name = std::string(event->name);

            if (src_type.find("_hourly_base") != std::string::npos && (event->mask & IN_CREATE)) {
                // New date directory created (midnight)
                std::string base_type = src_type.substr(0, src_type.find("_hourly_base"));
                std::string new_date_dir = path + "/" + name;
                add_watch(new_date_dir, base_type + "_date_dir", IN_CREATE);
                LOG_I("New date dir: %s", new_date_dir.c_str());
            }
            else if (src_type.find("_date_dir") != std::string::npos && (event->mask & IN_CREATE)) {
                // New hour file created
                std::string base_type = src_type.substr(0, src_type.find("_date_dir"));
                std::string new_hour_path = path + "/" + name;

                // Remove old hour-file watch
                for (auto wit = watch_map_.begin(); wit != watch_map_.end(); ++wit) {
                    if (wit->second.first == base_type && wit->first != wd) {
                        remove_watch(wit->first);
                        break;
                    }
                }

                add_watch(new_hour_path, base_type, IN_MODIFY);
                tailers_[base_type].open_at_start(new_hour_path);
                LOG_I("New hour file for %s: %s", base_type.c_str(), new_hour_path.c_str());
                // Read immediately
                read_and_dispatch(base_type);
            }
            else if ((src_type == "book" || src_type == "fills") && (event->mask & IN_MODIFY)) {
                read_and_dispatch(src_type);
            }

            offset += event_size;
        }
    }

    void read_and_dispatch(const std::string& src_type) {
        auto it = tailers_.find(src_type);
        if (it == tailers_.end()) return;
        char channel = (src_type == "book") ? 'D' : 'F';
        auto lines = it->second.read_new_lines();
        for (auto& line : lines)
            dispatcher_.dispatch(channel, line);
    }

    void start_rotation_timer() {
        rotation_timer_.expires_after(std::chrono::seconds(1));
        rotation_timer_.async_wait([this](beast::error_code ec) {
            if (ec) return;
            check_rotation();
            // Also poll tailers as safety net
            read_and_dispatch("book");
            read_and_dispatch("fills");
            start_rotation_timer();
        });
    }

    void check_rotation() {
        struct { std::string base; std::string src_type; } sources[] = {
            {book_base_, "book"},
            {fills_base_, "fills"},
        };

        for (auto& [base, src_type] : sources) {
            // Check for new date dir
            std::string date_dir = get_current_date_dir(base);
            std::string date_key = src_type + "_date_dir";
            bool has_date_watch = false;
            for (auto& [wd, info] : watch_map_) {
                if (info.first == date_key && info.second == date_dir) {
                    has_date_watch = true;
                    break;
                }
            }
            if (!has_date_watch) {
                struct stat st;
                if (stat(date_dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
                    add_watch(date_dir, date_key, IN_CREATE);
                    LOG_I("Rotation: watching new date dir %s", date_dir.c_str());
                }
            }

            // Check for new hour file
            std::string hour_path = get_current_hour_path(base);
            auto tailer_it = tailers_.find(src_type);
            if (tailer_it != tailers_.end() && tailer_it->second.path != hour_path) {
                struct stat st;
                if (stat(hour_path.c_str(), &st) == 0) {
                    // Remove old hour-file watch
                    for (auto wit = watch_map_.begin(); wit != watch_map_.end(); ++wit) {
                        if (wit->second.first == src_type) {
                            remove_watch(wit->first);
                            break;
                        }
                    }
                    add_watch(hour_path, src_type, IN_MODIFY);
                    tailer_it->second.open_at_start(hour_path);
                    LOG_I("Rotation: new hour file for %s: %s", src_type.c_str(), hour_path.c_str());
                    read_and_dispatch(src_type);
                }
            } else if (tailer_it == tailers_.end()) {
                struct stat st;
                if (stat(hour_path.c_str(), &st) == 0) {
                    add_watch(hour_path, src_type, IN_MODIFY);
                    tailers_[src_type].open_at_start(hour_path);
                    LOG_I("First hour file for %s: %s", src_type.c_str(), hour_path.c_str());
                    read_and_dispatch(src_type);
                }
            }
        }
    }
};

// ============================================================================
// WebSocket session
// ============================================================================

class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    WsSession(tcp::socket&& socket, Dispatcher& dispatcher)
        : client_(std::make_shared<Client>(std::move(socket)))
        , dispatcher_(dispatcher)
    {
    }

    void run() {
        // Set TCP_NODELAY
        beast::error_code ec;
        client_->ws.next_layer().socket().set_option(tcp::no_delay(true), ec);

        // Get remote address before the handshake
        auto ep = client_->ws.next_layer().socket().remote_endpoint(ec);
        if (!ec)
            client_->addr = ep.address().to_string() + ":" + std::to_string(ep.port());
        else
            client_->addr = "unknown";

        // Disable permessage-deflate
        websocket::permessage_deflate pmd;
        pmd.server_enable = false;
        pmd.client_enable = false;
        client_->ws.set_option(pmd);

        // Set auto-fragment off for lower latency
        client_->ws.auto_fragment(false);

        // Accept the WebSocket upgrade
        client_->ws.set_option(websocket::stream_base::decorator(
            [](websocket::response_type& res) {
                res.set(http::field::server, "hl_relay/cpp");
            }));

        auto self = shared_from_this();
        client_->ws.async_accept(
            [self](beast::error_code ec) {
                if (ec) {
                    LOG_D("WS accept error from %s: %s",
                          self->client_->addr.c_str(), ec.message().c_str());
                    return;
                }
                LOG_I("WS client connected: %s", self->client_->addr.c_str());
                self->start_subscription_deadline();
                self->do_read_subscription();
            });
    }

private:
    std::shared_ptr<Client> client_;
    Dispatcher& dispatcher_;
    beast::flat_buffer read_buf_;

    void start_subscription_deadline() {
        client_->deadline.expires_after(std::chrono::seconds(10));
        auto self = shared_from_this();
        client_->deadline.async_wait(
            [self](beast::error_code ec) {
                if (ec) return;  // cancelled = subscription arrived in time
                if (!self->client_->active) return;
                LOG_W("WS client %s: no subscription within 10s, disconnecting",
                      self->client_->addr.c_str());
                self->send_error("subscription required within 10s");
            });
    }

    void do_read_subscription() {
        auto self = shared_from_this();
        client_->ws.async_read(
            read_buf_,
            [self](beast::error_code ec, std::size_t) {
                if (ec) {
                    if (ec != websocket::error::closed &&
                        ec != net::error::operation_aborted)
                        LOG_D("WS read error from %s: %s",
                              self->client_->addr.c_str(), ec.message().c_str());
                    self->client_->active = false;
                    self->client_->deadline.cancel();
                    return;
                }
                self->client_->deadline.cancel();
                self->handle_subscription();
            });
    }

    void handle_subscription() {
        auto data = beast::buffers_to_string(read_buf_.data());
        read_buf_.consume(read_buf_.size());

        rapidjson::Document doc;
        doc.Parse(data.c_str(), data.size());
        if (doc.HasParseError() || !doc.IsObject()) {
            LOG_W("WS client %s: invalid subscription JSON", client_->addr.c_str());
            send_error("invalid subscription JSON");
            return;
        }

        // Parse coins
        auto coins_it = doc.FindMember("coins");
        if (coins_it != doc.MemberEnd() && coins_it->value.IsArray() && coins_it->value.Size() > 0) {
            for (rapidjson::SizeType i = 0; i < coins_it->value.Size(); ++i) {
                if (!coins_it->value[i].IsString()) {
                    send_error("coins must be array of strings");
                    return;
                }
                client_->coins.insert(std::string(
                    coins_it->value[i].GetString(),
                    coins_it->value[i].GetStringLength()));
            }
            client_->has_coin_filter = true;
            // Build sorted coin key for dedup cache
            std::vector<std::string> sorted_coins(client_->coins.begin(), client_->coins.end());
            std::sort(sorted_coins.begin(), sorted_coins.end());
            for (auto& c : sorted_coins) {
                if (!client_->coin_key.empty()) client_->coin_key += ',';
                client_->coin_key += c;
            }
        }

        // Parse channels
        auto ch_it = doc.FindMember("channels");
        if (ch_it != doc.MemberEnd() && ch_it->value.IsArray() && ch_it->value.Size() > 0) {
            std::unordered_set<char> parsed;
            for (rapidjson::SizeType i = 0; i < ch_it->value.Size(); ++i) {
                if (!ch_it->value[i].IsString()) continue;
                std::string ch_name(ch_it->value[i].GetString(), ch_it->value[i].GetStringLength());
                auto map_it = CHANNEL_MAP.find(ch_name);
                if (map_it != CHANNEL_MAP.end())
                    parsed.insert(map_it->second);
            }
            if (!parsed.empty())
                client_->channels = std::move(parsed);
        }

        // Register with dispatcher
        dispatcher_.add_client(client_);

        // Build confirmation message
        send_subscription_confirmation();

        // Enter read loop (handle ping/pong/close via Beast)
        do_read_loop();
    }

    void send_subscription_confirmation() {
        // Build response JSON
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("status"); writer.String("subscribed");

        writer.Key("coins");
        if (client_->has_coin_filter) {
            std::vector<std::string> sorted_coins(client_->coins.begin(), client_->coins.end());
            std::sort(sorted_coins.begin(), sorted_coins.end());
            writer.StartArray();
            for (auto& c : sorted_coins) writer.String(c.c_str());
            writer.EndArray();
        } else {
            writer.String("all");
        }

        writer.Key("channels");
        {
            std::vector<std::string> ch_names;
            for (char c : client_->channels) {
                if (c == 'D') ch_names.push_back("book");
                else if (c == 'F') ch_names.push_back("trade");
            }
            std::sort(ch_names.begin(), ch_names.end());
            writer.StartArray();
            for (auto& n : ch_names) writer.String(n.c_str());
            writer.EndArray();
        }
        writer.EndObject();

        std::string ctrl = "C 0 " + std::string(sb.GetString(), sb.GetSize()) + "\n";
        auto msg = std::make_shared<std::string>(std::move(ctrl));
        client_->enqueue(std::move(msg));

        // Log
        std::string coins_str = client_->has_coin_filter ? "" : "all";
        if (client_->has_coin_filter) {
            for (auto& c : client_->coins) {
                if (!coins_str.empty()) coins_str += ",";
                coins_str += c;
            }
        }
        std::string ch_str;
        for (char c : client_->channels) {
            if (!ch_str.empty()) ch_str += ",";
            ch_str += c;
        }
        LOG_I("WS client %s subscribed: coins=%s channels=%s",
              client_->addr.c_str(), coins_str.c_str(), ch_str.c_str());
    }

    void do_read_loop() {
        auto self = shared_from_this();
        client_->ws.async_read(
            read_buf_,
            [self](beast::error_code ec, std::size_t) {
                if (ec) {
                    if (ec != websocket::error::closed &&
                        ec != net::error::operation_aborted)
                        LOG_D("WS client %s disconnected: %s",
                              self->client_->addr.c_str(), ec.message().c_str());
                    self->cleanup();
                    return;
                }
                // Discard any data frames from the client (we don't expect any)
                self->read_buf_.consume(self->read_buf_.size());
                self->do_read_loop();
            });
    }

    void send_error(const char* msg) {
        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("status"); writer.String("error");
        writer.Key("msg"); writer.String(msg);
        writer.EndObject();

        std::string ctrl = "C 0 " + std::string(sb.GetString(), sb.GetSize()) + "\n";

        auto self = shared_from_this();
        auto buf = std::make_shared<std::string>(std::move(ctrl));
        client_->ws.text(true);
        client_->ws.async_write(
            net::buffer(*buf),
            [self, buf](beast::error_code, std::size_t) {
                // Close after sending error
                self->client_->ws.async_close(websocket::close_code::protocol_error,
                    [self](beast::error_code) {
                        self->cleanup();
                    });
            });
    }

    void cleanup() {
        client_->active = false;
        dispatcher_.remove_client(client_);
        LOG_I("WS client disconnected: %s", client_->addr.c_str());
    }
};

// ============================================================================
// WebSocket listener (acceptor)
// ============================================================================

class WsListener : public std::enable_shared_from_this<WsListener> {
public:
    WsListener(net::io_context& ioc, tcp::endpoint endpoint, Dispatcher& dispatcher)
        : ioc_(ioc)
        , acceptor_(ioc)
        , dispatcher_(dispatcher)
    {
        beast::error_code ec;
        acceptor_.open(endpoint.protocol(), ec);
        if (ec) {
            LOG_E("acceptor open: %s", ec.message().c_str());
            throw std::runtime_error("Failed to open acceptor: " + ec.message());
        }

        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        acceptor_.bind(endpoint, ec);
        if (ec) {
            LOG_E("acceptor bind on port %d: %s", endpoint.port(), ec.message().c_str());
            throw std::runtime_error("Failed to bind port " +
                std::to_string(endpoint.port()) + ": " + ec.message());
        }

        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        if (ec) {
            LOG_E("acceptor listen: %s", ec.message().c_str());
            throw std::runtime_error("Failed to listen: " + ec.message());
        }

        LOG_I("WebSocket server listening on %s:%d",
              endpoint.address().to_string().c_str(), endpoint.port());
    }

    void run() { do_accept(); }

    void stop() {
        beast::error_code ec;
        acceptor_.close(ec);
    }

private:
    net::io_context& ioc_;
    tcp::acceptor acceptor_;
    Dispatcher& dispatcher_;

    void do_accept() {
        auto self = shared_from_this();
        acceptor_.async_accept(
            net::make_strand(ioc_),
            [self](beast::error_code ec, tcp::socket socket) {
                if (ec) {
                    if (ec != net::error::operation_aborted)
                        LOG_E("Accept error: %s", ec.message().c_str());
                    return;
                }
                std::make_shared<WsSession>(std::move(socket), self->dispatcher_)->run();
                self->do_accept();
            });
    }
};

// ============================================================================
// Config
// ============================================================================

struct Config {
    std::string host = "0.0.0.0";
    uint16_t ws_port = 8766;
    std::string data_dir;
    std::string log_file;
    bool verbose = false;
    bool line_format = false;

    Config() {
        const char* home = getenv("HOME");
        data_dir = std::string(home ? home : "/root") + "/hl/data";
    }
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--ws-port" || arg == "--port") {
            if (i + 1 >= argc) {
                fprintf(stderr, "Missing value for %s\n", arg.c_str());
                exit(2);
            }
            std::string port_arg = argv[++i];
            size_t consumed = 0;
            unsigned long parsed = 0;
            bool valid = false;
            try {
                parsed = std::stoul(port_arg, &consumed, 10);
                valid = (consumed == port_arg.size() && parsed <= 65535UL);
            } catch (...) {
                valid = false;
            }
            if (!valid) {
                fprintf(stderr, "Invalid %s value: %s (expected integer 0-65535)\n",
                        arg.c_str(), port_arg.c_str());
                exit(2);
            }
            cfg.ws_port = static_cast<uint16_t>(parsed);
        }
        else if (arg == "--host" && i + 1 < argc)
            cfg.host = argv[++i];
        else if (arg == "--data-dir" && i + 1 < argc)
            cfg.data_dir = argv[++i];
        else if (arg == "--verbose" || arg == "-v")
            cfg.verbose = true;
        else if (arg == "--line-format" || arg == "-l")
            cfg.line_format = true;
        else if ((arg == "--log-file") && i + 1 < argc)
            cfg.log_file = argv[++i];
        else if (arg == "--help" || arg == "-h") {
            fprintf(stderr,
                "Usage: %s [options]\n"
                "  --ws-port PORT    WebSocket listen port (default: 8766)\n"
                "  --host ADDR       Bind address (default: 0.0.0.0)\n"
                "  --data-dir PATH   Node data directory (default: ~/hl/data)\n"
                "  --log-file PATH   Write logs to file in addition to stderr\n"
                "  --line-format,-l  Use line/single-event format (node_raw_book_diffs, node_fills)\n"
                "  --verbose, -v     Enable debug logging\n",
                argv[0]);
            exit(0);
        }
    }
    return cfg;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    Config cfg = parse_args(argc, argv);
    setup_logger(cfg.verbose, cfg.log_file);

    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    LOG_I("Starting hl_relay (C++): ws-port=%d data_dir=%s format=%s",
          cfg.ws_port, cfg.data_dir.c_str(), cfg.line_format ? "line" : "batch-by-block");

    // Verify data directory exists
    struct stat st;
    if (stat(cfg.data_dir.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        LOG_W("Data directory does not exist: %s", cfg.data_dir.c_str());
    }

    net::io_context ioc{1};  // single-threaded

    Dispatcher dispatcher;

    // Set up WebSocket listener — fatal if port is unavailable
    std::shared_ptr<WsListener> listener;
    try {
        listener = std::make_shared<WsListener>(
            ioc,
            tcp::endpoint(net::ip::make_address(cfg.host), cfg.ws_port),
            dispatcher);
    } catch (const std::exception& e) {
        LOG_E("Fatal: %s", e.what());
        return 1;
    }
    listener->run();

    // Set up inotify watcher
    InotifyWatcher watcher(ioc, dispatcher, cfg.data_dir, cfg.line_format);
    watcher.start();

    // Signal handling
    net::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](beast::error_code, int sig) {
        LOG_I("Received signal %d, shutting down", sig);
        listener->stop();
        watcher.stop();
        // Close all client connections
        for (auto& client : dispatcher.clients) {
            client->active = false;
            beast::error_code ec;
            client->ws.next_layer().socket().close(ec);
        }
        ioc.stop();
    });

    LOG_I("Event loop running");
    ioc.run();

    LOG_I("Shutdown complete");
    return 0;
}
