# Final disk measurement: issuer and io-wq both restricted to CPU 0

Run https://github.com/djosh34/tigerstore/actions/runs/42700129694, commit `9fc597db`. Three repetitions, median [min–max]. 128 sequential preallocated WAL-like groups per case; root ext4 barrier=1. io_uring_register_iowq_aff explicitly restricts workers to CPU 0.

## Buffered total process CPU (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 16.21 [15.88–18.39] | 17.35 [16.38–20.89] | 25.61 [25.45–36.67] | 59.98 [58.93–64.05] |
| uring linked | 20.73 [20.62–22.35] | 21.41 [20.47–23.15] | 29.65 [29.49–31.00] | 70.32 [68.11–78.62] |
| registered buffers | 21.19 [20.59–23.66] | 23.24 [21.40–23.28] | 31.49 [30.97–31.75] | 75.62 [70.87–79.25] |
| buffers+registered file | 19.54 [19.26–23.94] | 22.31 [21.22–22.97] | 31.01 [30.73–32.31] | 75.54 [71.61–80.33] |

## Buffered issuer thread CPU (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 16.19 [15.85–18.37] | 17.34 [16.36–20.84] | 25.59 [25.43–36.65] | 59.97 [58.90–64.03] |
| uring linked | 2.62 [2.52–2.99] | 2.69 [2.66–2.71] | 3.04 [3.02–3.56] | 5.38 [5.18–5.54] |
| registered buffers | 2.76 [2.62–2.89] | 2.65 [2.64–2.68] | 3.25 [3.24–3.54] | 5.32 [5.05–5.79] |
| buffers+registered file | 2.71 [2.53–2.75] | 2.61 [2.54–2.65] | 3.14 [3.04–3.22] | 5.43 [5.06–5.68] |

## Buffered wall latency (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 227.16 [184.96–261.08] | 216.47 [173.82–2525.17] | 351.93 [299.08–561.27] | 825.88 [821.65–829.85] |
| uring linked | 179.57 [165.89–242.65] | 183.48 [174.19–238.37] | 210.01 [208.52–273.99] | 748.76 [743.03–823.94] |
| registered buffers | 184.51 [180.16–219.02] | 186.09 [179.27–208.30] | 211.05 [209.31–212.51] | 745.80 [730.23–747.15] |
| buffers+registered file | 173.56 [170.87–201.18] | 176.02 [169.14–182.09] | 212.54 [209.87–215.97] | 746.12 [734.32–752.34] |

## O_DIRECT total process CPU (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 13.03 [12.71–13.84] | 13.77 [13.55–14.04] | 16.32 [15.84–16.66] | 27.85 [26.85–30.23] |
| uring linked | 16.42 [16.16–18.81] | 16.96 [16.68–17.62] | 19.45 [19.39–19.82] | 30.73 [30.04–31.42] |
| registered buffers | 16.21 [15.81–23.21] | 16.28 [16.25–16.62] | 16.77 [16.58–17.25] | 19.92 [19.76–21.83] |
| buffers+registered file | 16.17 [15.76–22.63] | 16.29 [15.93–16.88] | 16.51 [16.41–18.04] | 20.37 [19.97–21.91] |

## O_DIRECT issuer thread CPU (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 13.02 [12.69–13.82] | 13.74 [13.54–14.03] | 16.30 [15.81–16.64] | 27.83 [26.82–30.22] |
| uring linked | 10.27 [10.06–12.15] | 10.91 [10.76–11.37] | 13.49 [13.43–13.74] | 24.49 [23.65–24.92] |
| registered buffers | 10.12 [9.99–15.49] | 10.34 [10.22–10.68] | 10.88 [10.71–11.00] | 13.62 [13.44–15.21] |
| buffers+registered file | 10.10 [9.67–14.98] | 10.37 [9.94–10.71] | 10.59 [10.49–11.59] | 14.16 [13.70–15.28] |

## O_DIRECT wall latency (µs / group)

| Mode | 4 KiB | 16 KiB | 64 KiB | 256 KiB |
|---|---:|---:|---:|---:|
| pwrite+fdatasync | 183.75 [159.57–204.88] | 182.03 [166.13–231.96] | 204.23 [189.32–206.31] | 720.97 [719.41–759.58] |
| uring linked | 177.73 [159.87–2170.46] | 174.41 [170.25–190.33] | 200.40 [191.15–200.84] | 751.71 [743.88–755.94] |
| registered buffers | 165.60 [162.27–2207.79] | 183.28 [175.28–185.01] | 203.68 [203.23–206.13] | 756.77 [753.50–763.80] |
| buffers+registered file | 164.70 [164.13–2785.75] | 187.47 [165.71–197.29] | 206.38 [205.80–210.62] | 756.04 [753.77–757.65] |

All pwrite cases use 2 data-path syscalls/group; all linked ring variants use 1 enter/group, including fsync completion waits. There is still one DATASYNC operation per group. Process CPU includes io-wq work; issuer-only savings must not be mistaken for total CPU savings. Latency is the mean over 128 groups for each repetition, with spread across three repetition means, not percentile latency. Cloud-storage outliers are retained.
