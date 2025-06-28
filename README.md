# TieredSA: Optimizing Throughput and Write Amplification for Flash-based Caches with Multi-tiered Sets

Usage: main -D <device_path> -c <code_type> -l <workload_type> "
                   "[-p <population_file_path> -s <size_file_path> (for bench)] "
                   "[-y <trace_type> -f <trace_file_path> (for trace: twitter/meta)] "
                   "[-r <read_ratio> -o <object_size> (for rand)] "
                   "[-P (prefill)] [-z <zone_num>] [-t <thread_num>] [-T <run_time>]
