// hl_client.cc - Direct file-watching client for Hyperliquid node data
//
// Watches node data directories via inotify, maintains L2 book
// (price -> quantity), extracts trades, and measures latency.
//
// Build: g++ -std=c++17 -O2 -o hl_client hl_client.cc -Iinclude
// Usage: ./hl_client [--coins BTC,ETH] [--duration 60] [--levels 20]

#include <nlohmann/json.hpp>

#include <sys/inotify.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using json = nlohmann::json;
namespace fs = std::filesystem;

// ============================================================================
// Time utilities
// ============================================================================

static int64_t now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// Parse ISO 8601 timestamp: "2026-02-24T11:00:00.020409872"
static int64_t parse_timestamp_ms(const std::string& ts) {
    struct tm tm = {};
    int y, M, d, h, m, s;
    long nanos = 0;

    if (sscanf(ts.c_str(), "%d-%d-%dT%d:%d:%d", &y, &M, &d, &h, &m, &s) != 6)
        return 0;
    tm.tm_year = y - 1900;
    tm.tm_mon = M - 1;
    tm.tm_mday = d;
    tm.tm_hour = h;
    tm.tm_min = m;
    tm.tm_sec = s;

    auto dot_pos = ts.find('.');
    if (dot_pos != std::string::npos) {
        std::string frac = ts.substr(dot_pos + 1);
        while (frac.size() < 9) frac += '0';
        frac = frac.substr(0, 9);
        nanos = std::stol(frac);
    }

    time_t epoch = timegm(&tm);
    return epoch * 1000 + nanos / 1000000;
}

// ============================================================================
// Latency tracker with component breakdown
// ============================================================================

struct LatencyTracker {
    std::vector<int64_t> e2e_samples;
    std::vector<int64_t> node_samples;
    std::vector<int64_t> client_samples;

    void add(int64_t e2e, int64_t node_lat, int64_t client_lat) {
        e2e_samples.push_back(e2e);
        node_samples.push_back(node_lat);
        client_samples.push_back(client_lat);
    }

    static void print_one(const char* name, std::vector<int64_t>& v) {
        if (v.empty()) { std::cout << "  " << name << ": no samples\n"; return; }
        std::sort(v.begin(), v.end());
        int n = v.size();
        auto p = [&](double pct) -> int64_t {
            return v[std::min((int)std::ceil(pct * n) - 1, n - 1)];
        };
        std::cout << "  " << name << " (n=" << n << "): "
                  << "p25:" << p(0.25)
                  << " p50:" << p(0.50)
                  << " p75:" << p(0.75)
                  << " p99:" << p(0.99)
                  << " p999:" << p(0.999)
                  << " max:" << v.back() << "\n";
    }

    void print_stats(const char* label) {
        std::cout << label << ":\n";
        print_one("e2e       ", e2e_samples);
        print_one("node      ", node_samples);
        print_one("client    ", client_samples);
    }
};

// ============================================================================
// L2 Order Book: price -> total quantity (no per-order detail)
// ============================================================================

struct L2Level {
    double px;
    double qty;
};

struct L2Book {
    // bids: descending; asks: ascending
    std::map<double, double, std::greater<double>> bids;
    std::map<double, double> asks;
    // Minimal per-order tracking: only oid -> current_sz (needed for remove)
    std::unordered_map<uint64_t, double> oid_sz;

    // "new" diff: add order to book
    void on_new(uint64_t oid, char side, double px, double sz) {
        oid_sz[oid] = sz;
        if (side == 'B') bids[px] += sz;
        else              asks[px] += sz;
    }

    void add_to_level(char side, double px, double delta) {
        if (side == 'B') {
            bids[px] += delta;
            if (bids[px] <= 1e-9) bids.erase(px);
        } else {
            asks[px] += delta;
            if (asks[px] <= 1e-9) asks.erase(px);
        }
    }

    // "update" diff: delta computed from origSz/newSz in the diff itself
    void on_update(uint64_t oid, char side, double px, double orig_sz, double new_sz) {
        oid_sz[oid] = new_sz;
        add_to_level(side, px, new_sz - orig_sz);
    }

    // "remove" diff: need stored size to subtract
    void on_remove(uint64_t oid, char side, double px) {
        auto it = oid_sz.find(oid);
        if (it != oid_sz.end()) {
            add_to_level(side, px, -it->second);
            oid_sz.erase(it);
        }
        // else: order predates our start, skip (L2 converges over time)
    }

    // Handle update for unknown orders (started before us) — delta is still correct
    void on_update_unknown(uint64_t oid, char side, double px, double orig_sz, double new_sz) {
        oid_sz[oid] = new_sz;
        add_to_level(side, px, new_sz - orig_sz);
    }

    std::vector<L2Level> top_bids(int n) const {
        std::vector<L2Level> out;
        for (auto& [px, qty] : bids) {
            if ((int)out.size() >= n) break;
            if (qty > 1e-9) out.push_back({px, qty});
        }
        return out;
    }

    std::vector<L2Level> top_asks(int n) const {
        std::vector<L2Level> out;
        for (auto& [px, qty] : asks) {
            if ((int)out.size() >= n) break;
            if (qty > 1e-9) out.push_back({px, qty});
        }
        return out;
    }

    size_t tracked_orders() const { return oid_sz.size(); }
    size_t bid_levels() const { return bids.size(); }
    size_t ask_levels() const { return asks.size(); }
};

// ============================================================================
// Low-level file reader using POSIX I/O
// ============================================================================

struct FastFileReader {
    int fd = -1;
    off_t file_pos = 0;
    std::string line_buffer;
    static constexpr size_t READ_BUF_SIZE = 65536;
    char read_buf[READ_BUF_SIZE];

    void open_file(const std::string& path, bool seek_to_end = true) {
        close_file();
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd >= 0 && seek_to_end) {
            file_pos = ::lseek(fd, 0, SEEK_END);
        } else if (fd >= 0) {
            file_pos = 0;
        }
        line_buffer.clear();
    }

    void close_file() {
        if (fd >= 0) { ::close(fd); fd = -1; }
    }

    std::vector<std::string> read_new_lines() {
        std::vector<std::string> lines;
        if (fd < 0) return lines;

        ::lseek(fd, file_pos, SEEK_SET);
        while (true) {
            ssize_t n = ::read(fd, read_buf, READ_BUF_SIZE);
            if (n <= 0) break;
            line_buffer.append(read_buf, n);
            file_pos += n;
        }

        size_t pos = 0;
        while (true) {
            size_t nl = line_buffer.find('\n', pos);
            if (nl == std::string::npos) break;
            if (nl > pos) lines.emplace_back(line_buffer, pos, nl - pos);
            pos = nl + 1;
        }
        if (pos > 0) line_buffer.erase(0, pos);
        return lines;
    }

    ~FastFileReader() { close_file(); }
};

// ============================================================================
// File source: watches a node data directory
// ============================================================================

struct FileSource {
    std::string base_dir;
    std::string name;
    int inotify_fd;
    int current_wd = -1;
    std::string current_file;
    FastFileReader reader;

    FileSource(const std::string& base, const std::string& n, int ifd)
        : base_dir(base), name(n), inotify_fd(ifd) {}

    std::string find_latest_file() {
        std::string hourly = base_dir + "/hourly";
        if (!fs::exists(hourly)) return "";
        std::string latest_date;
        for (auto& entry : fs::directory_iterator(hourly)) {
            std::string dname = entry.path().filename().string();
            if (dname > latest_date) latest_date = dname;
        }
        if (latest_date.empty()) return "";

        std::string date_dir = hourly + "/" + latest_date;
        int latest_hour = -1;
        for (auto& entry : fs::directory_iterator(date_dir)) {
            try {
                int hour = std::stoi(entry.path().filename().string());
                if (hour > latest_hour) latest_hour = hour;
            } catch (...) {}
        }
        if (latest_hour < 0) return "";
        return date_dir + "/" + std::to_string(latest_hour);
    }

    void start_watching() {
        std::string file = find_latest_file();
        if (!file.empty()) switch_to_file(file);

        // Watch hourly base dir for new date directories (midnight rotation)
        std::string hourly = base_dir + "/hourly";
        inotify_add_watch(inotify_fd, hourly.c_str(), IN_CREATE);

        // Watch the latest date directory for new hour files
        std::string latest_date_dir;
        for (auto& entry : fs::directory_iterator(hourly)) {
            std::string p = entry.path().string();
            if (p > latest_date_dir) latest_date_dir = p;
        }
        if (!latest_date_dir.empty()) {
            inotify_add_watch(inotify_fd, latest_date_dir.c_str(),
                              IN_CREATE | IN_MODIFY | IN_CLOSE_WRITE);
        }
    }

    void switch_to_file(const std::string& path) {
        current_file = path;
        reader.open_file(path, true);
        std::string parent = fs::path(path).parent_path().string();
        int wd = inotify_add_watch(inotify_fd, parent.c_str(),
                                   IN_MODIFY | IN_CREATE | IN_CLOSE_WRITE);
        if (wd >= 0) current_wd = wd;
    }

    std::vector<std::string> read_new_lines() { return reader.read_new_lines(); }

    void check_for_new_file() {
        // Ensure the latest date dir is watched (handles midnight rotation)
        std::string hourly = base_dir + "/hourly";
        if (fs::exists(hourly)) {
            std::string latest_date_dir;
            for (auto& entry : fs::directory_iterator(hourly)) {
                std::string p = entry.path().string();
                if (p > latest_date_dir) latest_date_dir = p;
            }
            if (!latest_date_dir.empty()) {
                inotify_add_watch(inotify_fd, latest_date_dir.c_str(),
                                  IN_CREATE | IN_MODIFY | IN_CLOSE_WRITE);
            }
        }

        std::string latest = find_latest_file();
        if (!latest.empty() && latest != current_file) {
            std::cerr << "[" << name << "] Switching to: " << latest << "\n";
            switch_to_file(latest);
        }
    }
};

// ============================================================================
// Trade extraction
// ============================================================================

struct Trade {
    std::string coin;
    char side;
    double px;
    double sz;
    int64_t time_ms;
};

// ============================================================================
// Main
// ============================================================================

static volatile bool g_running = true;
void signal_handler(int) { g_running = false; }

struct Config {
    std::unordered_set<std::string> coins;
    int64_t duration_secs = 60;  // 0 = unlimited (Ctrl+C to stop)
    int n_levels = 20;
    int64_t outlier_threshold_ms = 500;
    int stats_interval_secs = 300;  // periodic stats dump interval
    std::string data_dir;
};

Config parse_args(int argc, char* argv[]) {
    Config cfg;
    cfg.data_dir = std::string(getenv("HOME")) + "/hl/data";
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--coins" && i + 1 < argc) {
            std::istringstream ss(argv[++i]);
            std::string coin;
            while (std::getline(ss, coin, ',')) cfg.coins.insert(coin);
        } else if (arg == "--duration" && i + 1 < argc) {
            cfg.duration_secs = std::stol(argv[++i]);
        } else if (arg == "--stats-interval" && i + 1 < argc) {
            cfg.stats_interval_secs = std::stoi(argv[++i]);
        } else if (arg == "--levels" && i + 1 < argc) {
            cfg.n_levels = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            cfg.data_dir = argv[++i];
        } else if (arg == "--outlier" && i + 1 < argc) {
            cfg.outlier_threshold_ms = std::stol(argv[++i]);
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "  --coins BTC,ETH      Coins to track (default: BTC,ETH)\n"
                      << "  --duration 86400     Duration in seconds (0 = unlimited)\n"
                      << "  --stats-interval 300 Periodic stats dump interval in seconds\n"
                      << "  --levels 20          L2 price levels\n"
                      << "  --outlier 500        Log outliers above this ms\n"
                      << "  --data-dir PATH      Node data directory\n";
            exit(0);
        }
    }
    if (cfg.coins.empty()) cfg.coins = {"BTC", "ETH"};
    return cfg;
}

// Process diffs: update L2 books directly from raw_book_diff events
// No order status matching needed — we get side/px from the diff event itself
int process_diffs(
    const json& batch,
    const std::unordered_set<std::string>& coins,
    std::unordered_map<std::string, L2Book>& books
) {
    int count = 0;
    for (auto& ev : batch["events"]) {
        const std::string& coin = ev["coin"].get_ref<const std::string&>();
        if (coins.find(coin) == coins.end()) continue;

        uint64_t oid = ev["oid"].get<uint64_t>();
        char side = ev["side"].get_ref<const std::string&>()[0];
        double px = std::stod(ev["px"].get_ref<const std::string&>());
        auto& diff = ev["raw_book_diff"];
        auto& book = books[coin];

        if (diff.is_string() && diff.get_ref<const std::string&>() == "remove") {
            book.on_remove(oid, side, px);
            count++;
        } else if (diff.is_object()) {
            if (diff.contains("new")) {
                double sz = std::stod(diff["new"]["sz"].get_ref<const std::string&>());
                book.on_new(oid, side, px, sz);
                count++;
            } else if (diff.contains("update")) {
                double orig_sz = std::stod(diff["update"]["origSz"].get_ref<const std::string&>());
                double new_sz = std::stod(diff["update"]["newSz"].get_ref<const std::string&>());
                book.on_update_unknown(oid, side, px, orig_sz, new_sz);
                count++;
            }
        }
    }
    return count;
}

std::vector<Trade> process_fills(
    const json& batch,
    const std::unordered_set<std::string>& coins
) {
    std::vector<Trade> trades;
    auto& events = batch["events"];
    for (size_t i = 0; i + 1 < events.size(); i += 2) {
        auto& fill1 = events[i][1];
        auto& fill2 = events[i + 1][1];

        const std::string& coin = fill1["coin"].get_ref<const std::string&>();
        if (coins.find(coin) == coins.end()) continue;

        const json* taker = &fill1;
        if (fill2.contains("crossed") && fill2["crossed"].get<bool>())
            taker = &fill2;

        Trade t;
        t.coin = coin;
        t.side = taker->at("side").get_ref<const std::string&>()[0];
        t.px = std::stod(fill1["px"].get_ref<const std::string&>());
        t.sz = std::stod(fill1["sz"].get_ref<const std::string&>());
        t.time_ms = fill1["time"].get<int64_t>();
        trades.push_back(std::move(t));
    }
    return trades;
}

int main(int argc, char* argv[]) {
    Config cfg = parse_args(argc, argv);

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "HL Client - Direct file watching (L2 mode)\n";
    std::cout << "Coins: ";
    for (auto& c : cfg.coins) std::cout << c << " ";
    std::cout << "\nDuration: " << (cfg.duration_secs == 0 ? "unlimited" : std::to_string(cfg.duration_secs) + "s")
              << " | Outlier: " << cfg.outlier_threshold_ms << "ms"
              << " | Stats every: " << cfg.stats_interval_secs << "s\n\n";

    int ifd = inotify_init1(IN_NONBLOCK);
    if (ifd < 0) { perror("inotify_init1"); return 1; }

    FileSource diffs_src(cfg.data_dir + "/node_raw_book_diffs_by_block", "diffs", ifd);
    FileSource fills_src(cfg.data_dir + "/node_fills_by_block", "fills", ifd);

    diffs_src.start_watching();
    fills_src.start_watching();

    std::cout << "Watching:\n"
              << "  diffs: " << diffs_src.current_file << "\n"
              << "  fills: " << fills_src.current_file << "\n\n";

    // L2 books per coin
    std::unordered_map<std::string, L2Book> books;
    for (auto& coin : cfg.coins) books[coin] = L2Book();

    LatencyTracker book_tracker, trade_tracker;

    auto start_time = std::chrono::steady_clock::now();
    int64_t blocks_processed = 0;
    int64_t trades_processed = 0;
    int64_t outlier_count = 0;
    int64_t parse_errors = 0;

    auto last_rotation_check = start_time;
    auto last_stats_dump = start_time;

    struct pollfd pfd;
    pfd.fd = ifd;
    pfd.events = POLLIN;
    char inotify_buf[4096];

    while (g_running) {
        auto now_tp = std::chrono::steady_clock::now();
        auto elapsed = now_tp - start_time;
        int64_t elapsed_secs = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
        if (cfg.duration_secs > 0 && elapsed_secs >= cfg.duration_secs)
            break;

        int ret = poll(&pfd, 1, 1);
        if (ret > 0 && (pfd.revents & POLLIN)) {
            while (true) {
                ssize_t len = ::read(ifd, inotify_buf, sizeof(inotify_buf));
                if (len <= 0) break;
                char* ptr = inotify_buf;
                while (ptr < inotify_buf + len) {
                    auto* event = reinterpret_cast<struct inotify_event*>(ptr);
                    if (event->mask & IN_CREATE) {
                        diffs_src.check_for_new_file();
                        fills_src.check_for_new_file();
                    }
                    ptr += sizeof(struct inotify_event) + event->len;
                }
            }
        }

        // Read and process diffs (no order_status matching needed!)
        for (auto& line : diffs_src.read_new_lines()) {
            try {
                auto batch = json::parse(line);
                int applied = process_diffs(batch, cfg.coins, books);

                if (applied > 0) {
                    // Read top-of-book (forces the L2 "computation" — it's just a map read)
                    for (auto& [coin, book] : books) {
                        auto bids = book.top_bids(cfg.n_levels);
                        auto asks = book.top_asks(cfg.n_levels);
                        (void)bids; (void)asks;
                    }

                    int64_t measure_time = now_ms();
                    int64_t block_time = parse_timestamp_ms(
                        batch["block_time"].get<std::string>());
                    int64_t local_time = parse_timestamp_ms(
                        batch["local_time"].get<std::string>());
                    int64_t e2e = measure_time - block_time;
                    int64_t node_lat = local_time - block_time;
                    int64_t client_lat = measure_time - local_time;
                    book_tracker.add(e2e, node_lat, client_lat);
                    blocks_processed++;

                    if (e2e > cfg.outlier_threshold_ms) {
                        std::cerr << "[BOOK OUTLIER] e2e=" << e2e
                                  << "ms node=" << node_lat
                                  << "ms client=" << client_lat
                                  << "ms bn=" << batch["block_number"]
                                  << "\n";
                        outlier_count++;
                    }

                    if (blocks_processed % 500 == 0) {
                        std::cout << "[" << elapsed_secs << "s] "
                                  << "Blocks: " << blocks_processed
                                  << " | Trades: " << trades_processed
                                  << " | Outliers: " << outlier_count
                                  << " | Errors: " << parse_errors;
                        for (auto& [c, b] : books) {
                            std::cout << " | " << c << ": "
                                      << b.bid_levels() << "b/"
                                      << b.ask_levels() << "a/"
                                      << b.tracked_orders() << "oids";
                        }
                        std::cout << " | lat=" << e2e << "ms\n";
                    }
                }
            } catch (const std::exception& e) {
                parse_errors++;
                if (parse_errors <= 10)
                    std::cerr << "[DIFF PARSE ERROR] " << e.what() << "\n";
            }
        }

        // Process fills
        for (auto& line : fills_src.read_new_lines()) {
            try {
                auto batch = json::parse(line);
                auto& events = batch["events"];
                if (events.empty()) continue;

                auto trades = process_fills(batch, cfg.coins);
                int64_t measure_time = now_ms();
                int64_t block_time = parse_timestamp_ms(batch["block_time"].get<std::string>());
                int64_t local_time = parse_timestamp_ms(batch["local_time"].get<std::string>());

                for (auto& t : trades) {
                    int64_t event_time = t.time_ms > 0 ? t.time_ms : block_time;
                    int64_t e2e = measure_time - event_time;
                    int64_t node_lat = local_time - event_time;
                    int64_t client_lat = measure_time - local_time;
                    trade_tracker.add(e2e, node_lat, client_lat);
                    trades_processed++;

                    if (e2e > cfg.outlier_threshold_ms) {
                        std::cerr << "[TRADE OUTLIER] e2e=" << e2e
                                  << "ms node=" << node_lat
                                  << "ms client=" << client_lat
                                  << "ms coin=" << t.coin << "\n";
                        outlier_count++;
                    }
                }
            } catch (const std::exception& e) {
                parse_errors++;
                if (parse_errors <= 10)
                    std::cerr << "[FILLS PARSE ERROR] " << e.what() << "\n";
            }
        }

        // Periodic rotation check (safety net — don't rely solely on inotify)
        if (std::chrono::duration_cast<std::chrono::seconds>(now_tp - last_rotation_check).count() >= 30) {
            diffs_src.check_for_new_file();
            fills_src.check_for_new_file();
            last_rotation_check = now_tp;
        }

        // Periodic stats dump
        if (cfg.stats_interval_secs > 0 &&
            std::chrono::duration_cast<std::chrono::seconds>(now_tp - last_stats_dump).count() >= cfg.stats_interval_secs) {
            std::cout << "\n--- Interim stats at " << elapsed_secs << "s ---\n";
            std::cout << "Blocks: " << blocks_processed
                      << " | Trades: " << trades_processed
                      << " | Outliers: " << outlier_count
                      << " | Parse errors: " << parse_errors << "\n";
            std::cout << "Watching:\n"
                      << "  diffs: " << diffs_src.current_file << "\n"
                      << "  fills: " << fills_src.current_file << "\n";
            for (auto& [coin, book] : books) {
                std::cout << coin << ": " << book.bid_levels() << " bid/"
                          << book.ask_levels() << " ask levels, "
                          << book.tracked_orders() << " tracked oids\n";
            }
            book_tracker.print_stats("Book");
            std::cout << "\n";
            trade_tracker.print_stats("Trade");
            std::cout << "---\n\n";
            last_stats_dump = now_tp;
        }
    }

    ::close(ifd);

    auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start_time).count();
    std::cout << "\n=== Latency Statistics ===\n";
    std::cout << "Duration: " << total_elapsed << "s"
              << " | Blocks: " << blocks_processed
              << " | Trades: " << trades_processed
              << " | Outliers(>" << cfg.outlier_threshold_ms << "ms): " << outlier_count
              << " | Parse errors: " << parse_errors << "\n\n";

    for (auto& [coin, book] : books) {
        auto bids = book.top_bids(3);
        auto asks = book.top_asks(3);
        std::cout << coin << ": " << book.bid_levels() << " bid levels, "
                  << book.ask_levels() << " ask levels, "
                  << book.tracked_orders() << " tracked oids\n";
        if (!bids.empty()) std::cout << "  best bid: " << bids[0].px << " x " << bids[0].qty << "\n";
        if (!asks.empty()) std::cout << "  best ask: " << asks[0].px << " x " << asks[0].qty << "\n";
    }
    std::cout << "\n";
    book_tracker.print_stats("Book");
    std::cout << "\n";
    trade_tracker.print_stats("Trade");

    return 0;
}
