# Assignment3-pdc: Parallel Web Crawlers

Coursework project comparing **four** crawl strategies over the same frontier: **sequential** BFS (rate-limited), **multi-threaded** BFS with a work-stealing queue and load monitor, **multiprocessing** with a managed shared frontier, and **Ray** actors (master/worker coordination). Supporting pieces include URL utilities, CSV metrics collection, Seaborn/Matplotlib figures, and a single CLI entrypoint.

---

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| Python 3.10+ | Matches type hints (`list[str]`, `\|` unions) |
| Network access | Targets like `books.toscrape.com` for real crawls |

---

## Installation

Clone or copy the repo, create a virtual environment, and install dependencies:

### Linux/macOS
```bash
cd assign3_pdc
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### Windows (PowerShell)
```powershell
cd assign3_pdc
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Windows (CMD)
```cmd
cd assign3_pdc
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**`requirements.txt`** currently includes: `requests`, `beautifulsoup4`, `lxml`, `ray`, `matplotlib`, `seaborn`, `pandas`, `tqdm`, `python-docx`.

**Note on Ray:** Ray may not be available on all platforms (e.g., Windows with Python 3.13+). If Ray installation fails, you can install dependencies without it:

```bash
pip install requests beautifulsoup4 lxml matplotlib seaborn pandas tqdm python-docx
```

The benchmark will automatically skip Ray tests if it's not available.

---

## Quick start (`main.py`)

**Important**: Make sure your virtual environment is activated before running commands!

Logging is configured automatically at **INFO** with format:

`%(asctime)s [%(levelname)s] %(name)s: %(message)s`

### Run one engine

```bash
python main.py --mode sequential --url https://books.toscrape.com --max-pages 50
python main.py --mode threaded --url https://books.toscrape.com --workers 8 --max-pages 100
python main.py --mode multiprocessing --workers 4 --max-pages 100
python main.py --mode ray --workers 8 --max-pages 80  # Skip if Ray not available
```

Defaults: `--url` is `https://books.toscrape.com`, `--workers` is **8**, `--max-pages` is **100**. The sequential crawler ignores `--workers`.

### Benchmark mode (timed sweep + plots)

Runs sequential once (**200 pages**), then threaded, multiprocessing, and Ray (if importable) for worker counts **`1, 2, 4, 8, 12, 16`**. Writes CSVs and PNGs via `visualize.generate_all_plots`:

```bash
python main.py --mode benchmark --url https://books.toscrape.com
```

Expected outputs:

| Path | Purpose |
|------|---------|
| `benchmark_runs.csv` | `crawler`, `num_workers`, `execution_time_sec`, `pages_per_second` |
| `metrics.csv` | Threaded-monitor style samples for utilization heatmaps |
| `plot_out/` | PNG figures (**150 DPI**): execution time vs workers, speedup, throughput bars, utilization heatmap |

### Word report (tables + embedded figures)

After you have `benchmark_runs.csv` (and optionally `metrics.csv` for a real heatmap):

```bash
python generate_report.py --refresh-plots -o Crawler_Benchmark_Report.docx
```

Omit `--refresh-plots` if `plot_out/*.png` are already current. Figures and tables are **machine-specific** until you regenerate them from `python main.py --mode benchmark`.

The `.docx` prints the four coursework PNGs **before** the scalability reading guide (`§7 Figures`, then `§8 How to read…`). PNGs load from `--plots-dir` or, if absent there, from the same folder as `benchmark_runs.csv`. Optional diagrams:

```bash
python generate_report.py --extra-image path/to/architecture.png -o Crawler_Benchmark_Report.docx
```

### Self-tests

```bash
python main.py --run-tests
```

Checks that major engines return at least **10** successful pages from Books to Scrape and that **dead URLs** are reflected in **`dead_urls.log`** (after hitting a bogus catalogue URL).

---

## Running modules standalone

Each crawler file can still be executed directly for quick experiments (see `if __name__ == "__main__"` blocks):

```bash
python crawler_sequential.py    https://books.toscrape.com --max-pages 25
python crawler_threaded.py      # benchmarks worker counts internally
python crawler_multiprocessing.py https://books.toscrape.com
python crawler_ray.py           https://books.toscrape.com
python visualize.py             # regenerate plots from CSVs next to cwd
```

---

## Project layout

| File | Role |
|------|------|
| `main.py` | Argparse dispatcher, benchmark orchestration, `run_all_tests()` |
| `crawler_sequential.py` | BFS + `fetch_page`, `dead_urls.log`, tqdm |
| `crawler_threaded.py` | Thread workers + `WorkStealingQueue` integration |
| `crawler_multiprocessing.py` | Manager-backed queue/results/visited + workers |
| `crawler_ray.py` | Ray `MasterActor` / `WorkerActor`, `crawl_ray()` |
| `load_balancer.py` | Work-stealing queue, adaptive batch sizing, CSV monitor thread |
| `utils.py` | `normalize_url`, `is_valid_url`, `RateLimiter` |
| `metrics.py` | `CrawlMetrics`, `MetricsCollector`, example benchmark CSV builder |
| `visualize.py` | `generate_all_plots()`, legacy bar helpers |
| `requirements.txt` | Pip dependencies |

---

## Sample benchmark table (illustrative)

Network and hardware dominate wall time; numbers are **fabricated** to show typical scaling shape.

```
   crawler num_workers execution_time_sec  pages_per_second
sequential           1                92.40             2.165
 threaded           1                58.90             3.397
 threaded           2                32.10             6.229
 threaded           4                19.05            10.499
 threaded           8                13.95            14.336
 threaded          12                12.85            15.563
 threaded          16                12.20            16.393
 multiprocessing    1                61.30             3.263
 multiprocessing    2                35.00             5.714
 multiprocessing    4                21.05             9.501
 multiprocessing    8                16.05            12.461
 multiprocessing   12                15.10            13.245
 multiprocessing   16                14.95            13.379
 ray                1                55.95             3.574
 ray                2                29.95             6.677
 ray                4                17.95            11.142
 ray                8                13.95            14.337
 ray               12                13.05            15.326
 ray               16                12.95            15.444
```

---

## Troubleshooting

### Ray Installation Issues
- **Windows Python 3.13+**: Ray is not currently available. Install other dependencies manually (see Installation section).
- **First run**: Ray may download binaries on first use. Be patient.
- **Import errors**: The benchmark automatically skips Ray if it cannot be imported.

### Platform-Specific Notes
- **Windows**: Use PowerShell or CMD commands shown in Installation section. Multiprocessing uses `spawn` start method.
- **macOS**: Multiprocessing uses `spawn` start method; ensure worker functions are module-level and Pickle-compatible.
- **Linux**: Should work out of the box with all features.

### Common Issues
- **Network errors**: Ensure you have internet access. The default target `books.toscrape.com` should be accessible.
- **Slow performance**: Network latency dominates. Results vary by connection speed and server load.
- **Missing plots**: Run `python main.py --mode benchmark` first to generate data and plots.

---

## Practical notes

- **Robots / etiquette:** Default seed is an educational scraping playground; still avoid hammering hosts. Sequential runs can use **`delay`** in-code; benchmarks use **`delay=0`** for coursework timing—do not use that against fragile sites without permission.
- **Performance**: Network latency and server response times dominate execution time. Your results will differ from sample data based on your connection and hardware.
