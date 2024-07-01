#include <iostream>
#include <lmdb.h>
#include <cstring>
#include <chrono>
#include <cassert>
#include <getopt.h>
#include <thread>

#define B 10240

uint64_t get_cur_us() {
    auto now = std::chrono::steady_clock::now();
    auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
    return now_us.count();
}

void dump_stat(MDB_env* env) {
    MDB_stat stat;
    mdb_env_stat(env, &stat);
    std::cout << "Page size: " << stat.ms_psize << std::endl;
    std::cout << "Depth: " << stat.ms_depth << std::endl;
    std::cout << "Branch pages: " << stat.ms_branch_pages << std::endl;
    std::cout << "Leaf pages: " << stat.ms_leaf_pages << std::endl;
    std::cout << "Overflow pages: " << stat.ms_overflow_pages << std::endl;
    std::cout << "Entries: " << stat.ms_entries << std::endl;
}

void handle_error(int rc, const char* msg) {
    if (rc != MDB_SUCCESS) {
        std::cerr << msg << ": " << mdb_strerror(rc) << std::endl;
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char** argv) {
    int c;
    unsigned int flags = 0;
    const char* filename = "test.db";
    MDB_env* env;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_txn* txn;

    while ((c = getopt(argc, argv, "nrf:")) != -1) {
        switch (c) {
        case 'n':
            flags |= MDB_NOSYNC;
            break;
        case 'f':
            filename = optarg;
            break;
        default:
            break;
        }
    }

    argc -= optind;
    argv += optind;

    if (argc == 0) {
        std::cerr << "Missing command" << std::endl;
        return 1;
    }

    int rc = mdb_env_create(&env);
    handle_error(rc, "Failed to create environment");
    
    rc = mdb_env_set_maxreaders(env, 1);
    handle_error(rc, "Failed to set max readers");

    rc = mdb_env_set_mapsize(env, 10485760);
    handle_error(rc, "Failed to set map size");

    rc = mdb_env_open(env, filename, flags, 0664);
    handle_error(rc, "Failed to open environment");

    rc = mdb_txn_begin(env, nullptr, 0, &txn);
    handle_error(rc, "Failed to begin transaction");

    rc = mdb_dbi_open(txn, nullptr, 0, &dbi);
    handle_error(rc, "Failed to open database");

    rc = mdb_txn_commit(txn);
    handle_error(rc, "Failed to commit transaction");

    if (strcmp(argv[0], "info") == 0) {
        dump_stat(env);
    }
    else if (strcmp(argv[0], "put") == 0) {
        if (argc < 3) {
            std::cerr << "Missing arguments" << std::endl;
            return 1;
        }
        uint64_t st = get_cur_us();
        key.mv_data = argv[1];
        key.mv_size = strlen(argv[1]);
        data.mv_data = argv[2];
        data.mv_size = strlen(argv[2]);

        rc = mdb_txn_begin(env, nullptr, 0, &txn);
        handle_error(rc, "Failed to begin transaction");

        rc = mdb_put(txn, dbi, &key, &data, 0);
        handle_error(rc, "Failed to put data");

        rc = mdb_txn_commit(txn);
        handle_error(rc, "Failed to commit transaction");

        uint64_t et = get_cur_us();
        std::cout << "put " << (et - st) << " us" << std::endl;
        std::cout << "OK" << std::endl;

        dump_stat(env);
    }
    else if (strcmp(argv[0], "putn") == 0) {
        if (argc < 4) {
            std::cerr << "Missing arguments" << std::endl;
            return 1;
        }
        int n = std::stoi(argv[3]);
        uint64_t st = get_cur_us();

        char buf_key[1024];
        char buf_val[1024];

        for (int rest = n, i = 0; rest > 0;) {
            rc = mdb_txn_begin(env, nullptr, 0, &txn);
            handle_error(rc, "Failed to begin transaction");

            int r = (rest < B) ? rest : B;
            for (int k = 0; k < r; ++k) {
                ++i;
                snprintf(buf_key, sizeof(buf_key), "%s_%d", argv[1], i);
                snprintf(buf_val, sizeof(buf_val), "%s_%d", argv[2], i);

                key.mv_data = buf_key;
                key.mv_size = strlen(buf_key);
                data.mv_data = buf_val;
                data.mv_size = strlen(buf_val);

                rc = mdb_put(txn, dbi, &key, &data, 0);
                assert(rc == MDB_SUCCESS);
            }
            rc = mdb_txn_commit(txn);
            handle_error(rc, "Failed to commit transaction");

            rest -= r;
        }

        uint64_t et = get_cur_us();
        std::cout << "putn " << (et - st) << " us" << std::endl;
        dump_stat(env);
    }
    else if (strcmp(argv[0], "get") == 0) {
        if (argc < 2) {
            std::cerr << "Missing arguments" << std::endl;
            return 1;
        }
        uint64_t st = get_cur_us();
        key.mv_data = argv[1];
        key.mv_size = strlen(argv[1]);

        rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        handle_error(rc, "Failed to begin read transaction");

        rc = mdb_get(txn, dbi, &key, &data);
        uint64_t et = get_cur_us();
        std::cout << "get " << (et - st) << " us" << std::endl;
        
        if (rc == 0) {
            std::cout << "OK " << std::string((char*)data.mv_data, data.mv_size) << std::endl;
        } else {
            std::cerr << "FAIL" << std::endl;
        }

        mdb_txn_abort(txn);
        dump_stat(env);
    }
    else if (strcmp(argv[0], "del") == 0) {
        if (argc < 2) {
            std::cerr << "Missing arguments" << std::endl;
            return 1;
        }
        key.mv_data = argv[1];
        key.mv_size = strlen(argv[1]);

        rc = mdb_txn_begin(env, nullptr, 0, &txn);
        handle_error(rc, "Failed to begin transaction");

        rc = mdb_del(txn, dbi, &key, nullptr);
        handle_error(rc, "Failed to delete data");

        rc = mdb_txn_commit(txn);
        handle_error(rc, "Failed to commit transaction");

        std::cout << "OK" << std::endl;

        dump_stat(env);
    }
    else if (strcmp(argv[0], "dels") == 0) {
        if (argc < 3) {
            std::cerr << "Missing arguments" << std::endl;
            return 1;
        }
        key.mv_data = argv[1];
        key.mv_size = strlen(argv[1]);
        MDB_val maxkey;
        maxkey.mv_data = argv[2];
        maxkey.mv_size = strlen(argv[2]);

        bool done = false;
        unsigned int del = 0;

        while (!done) {
            rc = mdb_txn_begin(env, nullptr, 0, &txn);
            handle_error(rc, "Failed to begin transaction");

            MDB_cursor* cursor;
            rc = mdb_cursor_open(txn, dbi, &cursor);
            handle_error(rc, "Failed to open cursor");

            rc = mdb_cursor_get(cursor, &key, nullptr, MDB_SET_RANGE);
            while (rc == MDB_SUCCESS) {
                if (mdb_cmp(txn, dbi, &key, &maxkey) > 0) {
                    done = true;
                    break;
                }
                rc = mdb_del(txn, dbi, &key, nullptr);
                handle_error(rc, "Failed to delete data");

                del++;
                rc = mdb_cursor_get(cursor, &key, nullptr, MDB_NEXT);
            }

            mdb_cursor_close(cursor);
            rc = mdb_txn_commit(txn);
            handle_error(rc, "Failed to commit transaction");

            done = rc != MDB_SUCCESS;
        }
        std::cout << "deleted " << del << " keys" << std::endl;
        dump_stat(env);
    }
    else if (strcmp(argv[0], "scan") == 0) {
        MDB_cursor* cursor;
        rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        handle_error(rc, "Failed to begin read transaction");

        rc = mdb_cursor_open(txn, dbi, &cursor);
        handle_error(rc, "Failed to open cursor");

        if (argc > 1) {
            key.mv_data = argv[1];
            key.mv_size = strlen(argv[1]);
            rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);
        } else {
            rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
        }

        while (rc == 0) {
            std::cout << "OK " << std::string((char*)key.mv_data, key.mv_size) << " " << data.mv_size << std::endl;
            rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        dump_stat(env);
    }
    else if (strcmp(argv[0], "scan2") == 0) {
        MDB_cursor* cursor;
        rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        handle_error(rc, "Failed to begin read transaction");

        rc = mdb_cursor_open(txn, dbi, &cursor);
        handle_error(rc, "Failed to open cursor");

        if (argc > 1) {
            key.mv_data = argv[1];
            key.mv_size = strlen(argv[1]);
            rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);
        } else {
            rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            while (rc == 0) {
                std::cout << "OK " << std::string((char*)key.mv_data, key.mv_size) << " " << data.mv_size << std::endl;
                rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        dump_stat(env);
    }
    else if (strcmp(argv[0], "verify") == 0) {
        rc = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        handle_error(rc, "Failed to begin read transaction");

        MDB_cursor* cursor;
        rc = mdb_cursor_open(txn, dbi, &cursor);
        handle_error(rc, "Failed to open cursor");

        MDB_val prev_key;
        memset(&prev_key, 0, sizeof(prev_key));
        unsigned int n = 0, checked = 0;

        rc = mdb_cursor_get(cursor, &key, nullptr, MDB_FIRST);
        while (rc == MDB_SUCCESS) {
            assert(key.mv_data);
            if (prev_key.mv_size > 0) {
                checked++;
                if (mdb_cmp(txn, dbi, &key, &prev_key) <= 0) {
                    std::cerr << "verify error, [" << std::string((char*)key.mv_data, key.mv_size) << "] : ["
                        << std::string((char*)prev_key.mv_data, prev_key.mv_size) << "]" << std::endl;
                }
            }
            prev_key = key;
            rc = mdb_cursor_get(cursor, &key, nullptr, MDB_NEXT);
            n++;
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        if (checked) ++checked;
        std::cout << "checked " << checked << "/" << n << std::endl;
    }
    else {
        std::cerr << argv[0] << ": invalid command" << std::endl;
    }

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);

    return 0;
}
