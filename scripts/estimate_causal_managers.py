#!/usr/bin/env python3
"""Estimar cuántos managers tienen insights con causalidad (PPL → MNS).

Ejecuta RCA (Root Cause Analysis) para una muestra de managers y devuelve
una tabla: manager, kpi_mapping, si tiene correlaciones causales, número de pares.

Uso:
  # Prueba piloto con 2–3 managers (secuencial)
  uv run python scripts/estimate_causal_managers.py --pilot

  # Hasta 500 managers con 4 workers
  uv run python scripts/estimate_causal_managers.py --limit 500 --workers 4

  # Guardar tabla en CSV
  uv run python scripts/estimate_causal_managers.py --limit 100 --output resultados_causal.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Asegurar imports desde el root del proyecto ppl
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(_PROJECT_ROOT / ".env", override=True)

from aily_data_access_layer.dal import Dal

from src.data import (
    get_manager_profile,
    list_managers,
    load_manager_domain_kpis,
    load_manager_team_kpis,
    run_correlation,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
_logger = logging.getLogger(__name__)

# Truncar código para la tabla (los hashes son largos)
CODE_DISPLAY_LEN = 20


def _count_causal_signals(result_df) -> tuple[int, int, int]:
    """Cuenta pares por nivel: N* (EE<=0.1), N** (0.1<EE<=0.3), N*** (EE>0.3). Igual criterio que CLI. NaN = *."""
    if result_df is None or len(result_df) == 0:
        return (0, 0, 0)
    ee = result_df.get("explained_entropy")
    if ee is None:
        return (len(result_df), 0, 0)
    ee = ee.fillna(-1)  # NaN → *
    n_star3 = int((ee > 0.3).sum())
    n_star2 = int(((ee > 0.1) & (ee <= 0.3)).sum())
    n_star1 = int((ee <= 0.1).sum())
    return (n_star1, n_star2, n_star3)


def _process_one_manager(
    manager_code: str,
    max_lag: int = 6,
    domain_cache: dict | None = None,
    domain_cache_lock: threading.Lock | None = None,
) -> dict:
    """Procesa un manager: mismo flujo que el CLI (perfil → KPIs → RCA). Crea su propio Dal (thread-safe).

    Si domain_cache y domain_cache_lock se pasan, se reutilizan domain KPIs por (kpi_mapping, geos)
    para no recalcular cuando varios managers comparten BU y países.

    Returns:
        Dict con: manager_code_short, kpi_mapping, geo_count, n_star1, n_star2, n_star3, causal_counts,
        has_causal_insights, n_correlations, status, error (opcional).
    """
    dal = Dal()
    short_code = (manager_code[:CODE_DISPLAY_LEN] + "…") if len(manager_code) > CODE_DISPLAY_LEN else manager_code

    out = {
        "manager_code_short": short_code,
        "manager_code_full": manager_code,
        "kpi_mapping": "",
        "kpi_mapping_search_text": "",
        "geo_count": 0,
        "n_star1": 0,
        "n_star2": 0,
        "n_star3": 0,
        "causal_counts": "",
        "has_causal_insights": False,
        "n_correlations": 0,
        "status": "ok",
        "error": None,
    }

    try:
        profile = get_manager_profile(manager_code, dal=dal)
        if profile is None:
            out["status"] = "no_profile"
            return out

        kpi_mapping = profile.get("kpi_mapping")
        geo_codes = profile.get("geo_codes") or []
        out["kpi_mapping_search_text"] = profile.get("kpi_mapping_search_text", "")

        if not kpi_mapping:
            out["status"] = "no_kpi_mapping"
            out["kpi_mapping"] = ""
            out["geo_count"] = len(geo_codes)
            return out

        out["kpi_mapping"] = kpi_mapping
        out["geo_count"] = len(geo_codes)

        ppl_data = load_manager_team_kpis(manager_code, dal=dal)
        if len(ppl_data) == 0:
            out["status"] = "no_ppl_data"
            return out

        # Cache de domain KPIs por (kpi_mapping, geos) para evitar cargas repetidas
        cache_key = (kpi_mapping, tuple(sorted(geo_codes)))
        domain_df = None
        if domain_cache is not None and domain_cache_lock is not None:
            with domain_cache_lock:
                domain_df = domain_cache.get(cache_key)
            if domain_df is not None:
                domain_df = domain_df  # reutilizar (lectura)
        if domain_df is None:
            domain_df = load_manager_domain_kpis(kpi_mapping, geo_codes, dal=dal)
            if domain_cache is not None and domain_cache_lock is not None and len(domain_df) > 0:
                with domain_cache_lock:
                    domain_cache.setdefault(cache_key, domain_df)

        if len(domain_df) == 0:
            out["status"] = "no_domain_data"
            return out

        result = run_correlation(ppl_data, domain_df, max_lag=max_lag)
        n_corr = len(result)
        out["n_correlations"] = n_corr
        out["has_causal_insights"] = n_corr > 0
        n1, n2, n3 = _count_causal_signals(result)
        out["n_star1"], out["n_star2"], out["n_star3"] = n1, n2, n3
        out["causal_counts"] = f"{n1}  {n2}  {n3}" if n_corr > 0 else ""
        out["status"] = "ok"
        return out

    except Exception as e:
        _logger.exception("Manager %s: %s", short_code, e)
        out["status"] = "error"
        out["error"] = str(e)
        return out


def _print_table(rows: list[dict], use_tabulate: bool = True) -> None:
    """Imprime la tabla en consola."""
    if not rows:
        print("No hay filas que mostrar.", flush=True)
        return

    def _cell(c: str, r: dict) -> str:
        val = r.get(c, "")
        s = str(val).replace("\n", " ").replace("\r", " ").strip()
        if c == "kpi_mapping_search_text" and len(s) > 40:
            return s[:37] + "…"
        return s

    # Search text al final y truncado para que la tabla no se desborde en consola
    display_cols = [
        "manager_code_short",
        "kpi_mapping",
        "geo_count",
        "causal_counts",
        "n_correlations",
        "status",
        "kpi_mapping_search_text",
    ]
    headers = ["Manager", "KPI mapping", "Geos", "N* N** N***", "N correl.", "Status", "Search text"]

    if use_tabulate:
        try:
            from tabulate import tabulate
            data = [[_cell(c, r) for c in display_cols] for r in rows]
            print(flush=True)
            print(tabulate(data, headers=headers, tablefmt="simple", showindex=False, maxcolwidths=[None, None, None, None, None, None, 40]), flush=True)
            print(flush=True)
            print("  N* N** N*** = pares con EE<=0.1, 0.1<EE<=0.3, EE>0.3 (mismo criterio que CLI)", flush=True)
            print("  Search text = string usado para matching; añadir reglas en src/config.py KPI_MAPPING_RULES (p. ej. MSLT_OTIF)", flush=True)
            return
        except ImportError:
            pass
        except Exception:
            pass  # fallback si tabulate falla con algún valor

    # Fallback: tabla manual (ancho fijo para Search text)
    col_max = 40 if "kpi_mapping_search_text" in display_cols else 999
    widths = []
    for c in display_cols:
        w = max(len(_cell(c, r)) for r in rows) if rows else 0
        if c == "kpi_mapping_search_text":
            w = min(w, col_max)
        widths.append(max(w, len(headers[display_cols.index(c)])))
    fmt = "  ".join("{{:{}}}".format(w) for w in widths)
    print(flush=True)
    print(fmt.format(*headers), flush=True)
    print("-" * (sum(widths) + 2 * (len(widths) - 1)), flush=True)
    for r in rows:
        print(fmt.format(*[_cell(c, r) for c in display_cols]), flush=True)
    print(flush=True)
    print("  N* N** N*** = pares con EE<=0.1, 0.1<EE<=0.3, EE>0.3 (mismo criterio que CLI)", flush=True)
    print("  Search text = string usado para matching; añadir reglas en src/config.py KPI_MAPPING_RULES (p. ej. MSLT_OTIF)", flush=True)


def _normalize_row_from_csv(row: dict) -> dict:
    """Convierte tipos leídos del CSV (strings) a int/bool para resumen y tabla."""
    out = dict(row)
    for col in ("geo_count", "n_star1", "n_star2", "n_star3", "n_correlations"):
        val = out.get(col, 0)
        try:
            out[col] = int(val) if val != "" and val is not None else 0
        except (TypeError, ValueError):
            out[col] = 0
    val = out.get("has_causal_insights", False)
    out["has_causal_insights"] = str(val).strip().lower() in ("true", "1", "yes")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Estimar managers con relaciones causales PPL → MNS (RCA).",
    )
    parser.add_argument(
        "--pilot",
        action="store_true",
        help="Modo piloto: solo 3 managers, 1 worker (para probar).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        metavar="N",
        help="Máximo de managers a procesar (default: 500). Con --pilot se ignora y se usa 3.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="W",
        help="Número de workers en paralelo (default: 4). Con --pilot se usa 1.",
    )
    parser.add_argument(
        "--max-lag",
        type=int,
        default=6,
        help="Máximo lag en meses para Granger (default: 6).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        metavar="FILE",
        help="Ruta CSV opcional para guardar la tabla.",
    )
    parser.add_argument(
        "--include-non-managers",
        action="store_true",
        help="Incluir empleados que no son managers (is_manager=FALSE).",
    )
    args = parser.parse_args()

    if args.pilot:
        limit = 3
        workers = 1
        _logger.info("Modo piloto: limit=3, workers=1")
    else:
        limit = args.limit
        workers = max(1, args.workers)

    managers_df = list_managers(
        limit=limit,
        is_manager_only=not args.include_non_managers,
    )
    if len(managers_df) == 0:
        _logger.error("No se encontraron managers. Ajusta filtros o base de datos.")
        sys.exit(1)

    all_manager_codes = managers_df["employee_code"].tolist()

    # Si -o y el archivo existe, cargar filas ya guardadas y no re-ejecutar esos managers
    existing_rows: list[dict] = []
    outpath = Path(args.output) if args.output else None
    if outpath and outpath.exists():
        import csv
        with open(outpath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                existing_rows.append(_normalize_row_from_csv(r))
        existing_codes = {r["manager_code_full"] for r in existing_rows if r.get("manager_code_full")}
        manager_codes = [mc for mc in all_manager_codes if mc not in existing_codes]
        _logger.info(
            "CSV existente: %d filas. Omitiendo %d manager(s) ya guardados. A ejecutar: %d.",
            len(existing_rows), len(existing_codes), len(manager_codes),
        )
    else:
        manager_codes = all_manager_codes

    domain_cache: dict = {}
    if not manager_codes:
        _logger.info("Nada nuevo que procesar (todos ya están en %s).", outpath)
        rows = existing_rows
        rows.sort(key=lambda r: r.get("manager_code_full", ""))
    else:
        _logger.info("Procesando %d manager(s) con %d worker(s) (cache domain KPIs activo) …", len(manager_codes), workers)
        domain_cache_lock = threading.Lock()
        new_rows: list[dict] = []
        if workers <= 1:
            for mc in manager_codes:
                new_rows.append(_process_one_manager(mc, args.max_lag, domain_cache, domain_cache_lock))
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_process_one_manager, mc, args.max_lag, domain_cache, domain_cache_lock): mc
                    for mc in manager_codes
                }
                for fut in as_completed(futures):
                    try:
                        new_rows.append(fut.result())
                    except Exception as e:
                        mc = futures[fut]
                        short = (mc[:CODE_DISPLAY_LEN] + "…") if len(mc) > CODE_DISPLAY_LEN else mc
                        _logger.exception("Worker failed for %s: %s", short, e)
                        new_rows.append({
                        "manager_code_short": short,
                        "manager_code_full": mc,
                        "kpi_mapping": "",
                        "kpi_mapping_search_text": "",
                        "geo_count": 0,
                        "n_star1": 0,
                        "n_star2": 0,
                        "n_star3": 0,
                        "causal_counts": "",
                        "has_causal_insights": False,
                        "n_correlations": 0,
                        "status": "error",
                        "error": str(e),
                    })
        rows = existing_rows + new_rows
        rows.sort(key=lambda r: r.get("manager_code_full", ""))

    # Resumen: managers con causal; total de pares por nivel (N* N** N***)
    n_ok = sum(1 for r in rows if r["status"] == "ok")
    n_causal = sum(1 for r in rows if r.get("has_causal_insights"))
    tot_star1 = sum(r.get("n_star1", 0) for r in rows)
    tot_star2 = sum(r.get("n_star2", 0) for r in rows)
    tot_star3 = sum(r.get("n_star3", 0) for r in rows)
    n_skip_kpi = sum(1 for r in rows if r["status"] == "no_kpi_mapping")
    n_skip_data = sum(1 for r in rows if r["status"] in ("no_ppl_data", "no_domain_data", "no_profile"))
    n_errors = sum(1 for r in rows if r["status"] == "error")
    n_cache_hits = len(domain_cache)

    # Resumen y tabla siempre visibles (flush para que no se pierdan entre logs del RCA)
    def _out(*args, **kwargs):
        kwargs.setdefault("flush", True)
        print(*args, **kwargs)

    _out()
    _out("=" * 60)
    _out("RESUMEN — Estimación causal PPL → MNS (mismo criterio que CLI)")
    _out("=" * 60)
    _out(f"  Total procesados:      {len(rows)}")
    _out(f"  Con RCA ejecutado:     {n_ok}")
    pct = (100 * n_causal / len(rows)) if rows else 0
    _out(f"  Con relación causal:   {n_causal}  ({pct:.1f}%)  → pares * {tot_star1}  ** {tot_star2}  *** {tot_star3}")
    _out(f"  Cache domain KPIs:    {n_cache_hits} claves (evita recargas)")
    _out(f"  Sin KPI mapping:      {n_skip_kpi}")
    _out(f"  Sin datos PPL/domain: {n_skip_data}")
    _out(f"  Errores:              {n_errors}")
    _out("=" * 60)
    _out()
    _print_table(rows)
    sys.stdout.flush()

    if args.output:
        import csv
        outpath = Path(args.output)
        # Mismo orden de columnas que genera _process_one_manager
        all_cols = [
            "manager_code_short", "manager_code_full", "kpi_mapping", "kpi_mapping_search_text",
            "geo_count", "n_star1", "n_star2", "n_star3", "causal_counts", "has_causal_insights",
            "n_correlations", "status", "error",
        ]
        with open(outpath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        _logger.info("Tabla guardada en %s (%d filas)", outpath, len(rows))


if __name__ == "__main__":
    main()
