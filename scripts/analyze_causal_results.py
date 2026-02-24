#!/usr/bin/env python3
"""Análisis de resultados del survey causal (resultados.csv).

Métricas:
  - % con coverage (has_causal_insights=True)
  - % con al menos 1 *, 1 **, 1 *** (entre quienes tienen RCA)
  - Desglose por status
  - Histograma: distribución de nº de insights (n_correlations) por manager

Uso:
  uv run python scripts/analyze_causal_results.py
  uv run python scripts/analyze_causal_results.py resultados.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Path por defecto
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_CSV = PROJECT_ROOT / "resultados.csv"


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Tipos numéricos
    for col in ("n_star1", "n_star2", "n_star3", "n_correlations", "geo_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    if "has_causal_insights" in df.columns:
        df["has_causal_insights"] = df["has_causal_insights"].astype(str).str.lower().isin(("true", "1", "yes"))
    return df


def pct(n: int, total: int) -> str:
    if total == 0:
        return "N/A"
    return f"{100 * n / total:.1f}%"


def run_analysis(df: pd.DataFrame) -> None:
    n_total = len(df)
    print()
    print("=" * 70)
    print("ANÁLISIS — Survey causal PPL → MNS (resultados.csv)")
    print("=" * 70)
    print(f"  Total filas: {n_total}")
    print()

    # --- 1. Coverage y % con *, **, *** (global) ---
    n_coverage = df["has_causal_insights"].sum()
    n_with_star = (df["n_star1"] > 0).sum()
    n_with_star2 = (df["n_star2"] > 0).sum()
    n_with_star3 = (df["n_star3"] > 0).sum()

    print("  [1] GLOBAL (todas las filas)")
    print("  ─────────────────────────────")
    print(f"      Coverage (has_causal=True):  {n_coverage:>5}  →  {pct(n_coverage, n_total)}")
    print(f"      Con al menos 1 *:             {n_with_star:>5}  →  {pct(n_with_star, n_total)}")
    print(f"      Con al menos 1 **:            {n_with_star2:>5}  →  {pct(n_with_star2, n_total)}")
    print(f"      Con al menos 1 ***:           {n_with_star3:>5}  →  {pct(n_with_star3, n_total)}")
    print()

    # Solo quienes tienen RCA ejecutado (status=ok) para % sobre "con mapping"
    df_ok = df[df["status"] == "ok"]
    n_ok = len(df_ok)
    if n_ok > 0:
        print("  [2] CUANDO HAY MAPPING (status=ok)")
        print("  ─────────────────────────────────")
        print(f"      Filas con status=ok:        {n_ok:>5}  →  {pct(n_ok, n_total)} del total")
        c_ok = df_ok["has_causal_insights"].sum()
        s1 = (df_ok["n_star1"] > 0).sum()
        s2 = (df_ok["n_star2"] > 0).sum()
        s3 = (df_ok["n_star3"] > 0).sum()
        print(f"      Coverage (has_causal=True): {c_ok:>5}  →  {pct(c_ok, n_ok)} de los con mapping")
        print(f"      Con al menos 1 *:           {s1:>5}  →  {pct(s1, n_ok)}")
        print(f"      Con al menos 1 **:          {s2:>5}  →  {pct(s2, n_ok)}")
        print(f"      Con al menos 1 ***:         {s3:>5}  →  {pct(s3, n_ok)}")
        print()

    # --- 3. Desglose por status ---
    print("  [3] DESGLOSE POR STATUS")
    print("  ───────────────────────")
    by_status = df.groupby("status", dropna=False).agg(
        count=("status", "count"),
        has_causal=("has_causal_insights", "sum"),
        n_star1_any=("n_star1", lambda s: (s > 0).sum()),
        n_star2_any=("n_star2", lambda s: (s > 0).sum()),
        n_star3_any=("n_star3", lambda s: (s > 0).sum()),
    ).reset_index()
    by_status["% total"] = by_status["count"].apply(lambda c: pct(int(c), n_total))
    by_status["% coverage"] = by_status.apply(lambda r: pct(int(r["has_causal"]), int(r["count"])), axis=1)
    by_status["% con *"] = by_status.apply(lambda r: pct(int(r["n_star1_any"]), int(r["count"])), axis=1)
    by_status["% con **"] = by_status.apply(lambda r: pct(int(r["n_star2_any"]), int(r["count"])), axis=1)
    by_status["% con ***"] = by_status.apply(lambda r: pct(int(r["n_star3_any"]), int(r["count"])), axis=1)

    cols_show = ["status", "count", "% total", "has_causal", "% coverage", "% con *", "% con **", "% con ***"]
    print(by_status[[c for c in cols_show if c in by_status.columns]].to_string(index=False))
    print()

    # --- 4. Histogramas: n_correlations y por *, **, *** (bins automáticos) ---
    print("  [4] DISTRIBUCIÓN DE INSIGHTS POR MANAGER (n_correlations)")
    print("  ───────────────────────────────────────────────────────")
    insights = df_ok["n_correlations"] if len(df_ok) > 0 else pd.Series(dtype=int)
    if len(insights) == 0:
        print("      No hay filas con status=ok para histograma.")
    else:
        bins_console = [0, 1, 5, 10, 20, 50, 100, 500, 10_000]
        hist = pd.cut(insights, bins=bins_console, include_lowest=True)
        counts = hist.value_counts().sort_index()
        max_count = counts.max() if len(counts) > 0 else 1
        bar_width = 40
        for interval, count in counts.items():
            pct_val = 100 * count / len(insights)
            bar_len = int(bar_width * count / max_count) if max_count > 0 else 0
            bar = "█" * bar_len
            print(f"      {str(interval):>20}  {count:>5}  ({pct_val:5.1f}%)  {bar}")
        print(f"      {'(solo status=ok)':>20}  {len(insights):>5}  total")
        print()

        try:
            import matplotlib.pyplot as plt
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))

            def plot_hist(ax, series, title, xlabel):
                series = series.dropna()
                if len(series) == 0:
                    ax.text(0.5, 0.5, "Sin datos", ha="center", va="center")
                    return
                # Bins automáticos (entero = nº de intervalos), escala natural
                ax.hist(series, bins=min(50, max(int(series.max()), 1)), edgecolor="black", alpha=0.7)
                ax.set_xlabel(xlabel)
                ax.set_ylabel("Nº de managers")
                ax.set_title(title)
                ax.grid(True, alpha=0.3)

            plot_hist(axes[0, 0], insights, "Total insights (n_correlations)", "Nº de pares PPL–MNS")
            plot_hist(axes[0, 1], df_ok["n_star1"], "Pares * (EE≤0.1)", "Nº de pares *")
            plot_hist(axes[1, 0], df_ok["n_star2"], "Pares ** (0.1<EE≤0.3)", "Nº de pares **")
            plot_hist(axes[1, 1], df_ok["n_star3"], "Pares *** (EE>0.3)", "Nº de pares ***")

            plt.suptitle("Distribución por manager (status=ok)", fontsize=12)
            plt.tight_layout()
            out_hist = PROJECT_ROOT / "resultados_causal_histogram.png"
            fig.savefig(out_hist, dpi=150, bbox_inches="tight")
            plt.close(fig)
            print(f"      Histogramas guardados: {out_hist} (4 paneles: total, *, **, ***)")
        except ImportError:
            print("      (Instala matplotlib para guardar los histogramas como PNG)")
    print()
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analizar resultados del survey causal (resultados.csv)")
    parser.add_argument("csv", nargs="?", default=str(DEFAULT_CSV), help="Ruta al CSV (default: resultados.csv)")
    args = parser.parse_args()

    path = Path(args.csv)
    if not path.exists():
        print(f"Error: no existe {path}", file=sys.stderr)
        sys.exit(1)

    df = load_csv(path)
    if "has_causal_insights" not in df.columns or "status" not in df.columns:
        print("Error: el CSV debe tener columnas has_causal_insights y status.", file=sys.stderr)
        sys.exit(1)

    run_analysis(df)


if __name__ == "__main__":
    main()
