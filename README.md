#!/usr/bin/env python3
"""Analisi ingombro percorso da file Microsoft Access (.mdb/.accdb).

Il modulo può essere usato sia come script CLI sia importato da altri moduli.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PARAMS: Dict[str, float | str] = {
    "Xmin": 0.0,
    "Xmax": 3000.0,
    "Ymin": 0.0,
    "Y1_max": 550.0,
    "Y2_max": 140.0,
    "Zmin": 0.0,
    "Zmax": 180.0,
    "OD": 50.0,
    "clearance": 2.0,
    "YVarAlong": "Z",  # 'Z' oppure 'X'
}


@dataclass
class AnalysisResult:
    file_path: str
    table_name: Optional[str]
    point_count: int
    status: str
    message: str
    dx: float = 0.0
    dy: float = 0.0
    dz: float = 0.0
    violation: float = 0.0
    xmin: float = math.nan
    xmax: float = math.nan
    ymin: float = math.nan
    ymax: float = math.nan
    zmin: float = math.nan
    zmax: float = math.nan
    xmin_shifted: float = math.nan
    xmax_shifted: float = math.nan
    ymin_shifted: float = math.nan
    ymax_shifted: float = math.nan
    zmin_shifted: float = math.nan
    zmax_shifted: float = math.nan


class MDBReadError(RuntimeError):
    """Errore durante lettura MDB."""


class BaseMDBReader:
    def list_tables(self) -> List[str]:
        raise NotImplementedError

    def list_columns(self, table_name: str) -> List[Tuple[str, Optional[str]]]:
        raise NotImplementedError

    def fetch_columns(self, table_name: str, columns: Sequence[str]) -> List[Tuple[float, float, float]]:
        raise NotImplementedError

    def close(self) -> None:
        return None


class ODBCMDBReader(BaseMDBReader):
    def __init__(self, file_path: str) -> None:
        try:
            import pyodbc
        except ImportError as exc:  # pragma: no cover - dipende dall'ambiente
            raise MDBReadError("pyodbc non disponibile") from exc

        self._pyodbc = pyodbc
        conn_str = (
            "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={file_path};"
        )
        try:
            self.conn = pyodbc.connect(conn_str, autocommit=True)
        except Exception as exc:  # pragma: no cover - dipende dal driver locale
            raise MDBReadError(f"Connessione ODBC fallita: {exc}") from exc

    def list_tables(self) -> List[str]:
        cursor = self.conn.cursor()
        tables = []
        for row in cursor.tables(tableType="TABLE"):
            name = getattr(row, "table_name", None) or row[2]
            if name:
                tables.append(str(name))
        return tables

    def list_columns(self, table_name: str) -> List[Tuple[str, Optional[str]]]:
        cursor = self.conn.cursor()
        cols: List[Tuple[str, Optional[str]]] = []
        for row in cursor.columns(table=table_name):
            name = getattr(row, "column_name", None) or row[3]
            tname = getattr(row, "type_name", None)
            cols.append((str(name), tname if tname is None else str(tname)))
        return cols

    def fetch_columns(self, table_name: str, columns: Sequence[str]) -> List[Tuple[float, float, float]]:
        cursor = self.conn.cursor()
        quoted_cols = ", ".join(f"[{c}]" for c in columns)
        sql = f"SELECT {quoted_cols} FROM [{table_name}]"
        try:
            rows = cursor.execute(sql).fetchall()
        except Exception as exc:
            raise MDBReadError(f"Query fallita ({sql}): {exc}") from exc

        out: List[Tuple[float, float, float]] = []
        for row in rows:
            try:
                x, y, z = (float(row[0]), float(row[1]), float(row[2]))
            except (TypeError, ValueError):
                continue
            out.append((x, y, z))
        return out

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class MezaMDBReader(BaseMDBReader):
    """Fallback basato su meza + mdbtools."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        try:
            from meza import process as meza_process
        except ImportError as exc:  # pragma: no cover
            raise MDBReadError("meza non disponibile per fallback") from exc
        self.meza_process = meza_process

        try:
            self._tables = list(meza_process.read_any(file_path, ext="mdb", sanitize=True))
        except Exception as exc:  # pragma: no cover
            raise MDBReadError(f"Lettura meza/mdbtools fallita: {exc}") from exc

    def list_tables(self) -> List[str]:
        return [t.get("name", f"table_{idx}") for idx, t in enumerate(self._tables)]

    def list_columns(self, table_name: str) -> List[Tuple[str, Optional[str]]]:
        table = self._get_table(table_name)
        rows = table.get("rows") or []
        if not rows:
            return []
        sample = rows[0]
        cols: List[Tuple[str, Optional[str]]] = []
        for key, value in sample.items():
            cols.append((str(key), type(value).__name__))
        return cols

    def fetch_columns(self, table_name: str, columns: Sequence[str]) -> List[Tuple[float, float, float]]:
        table = self._get_table(table_name)
        rows = table.get("rows") or []
        out: List[Tuple[float, float, float]] = []
        for row in rows:
            try:
                x = float(row[columns[0]])
                y = float(row[columns[1]])
                z = float(row[columns[2]])
            except (KeyError, TypeError, ValueError):
                continue
            out.append((x, y, z))
        return out

    def _get_table(self, table_name: str) -> Dict[str, object]:
        for idx, table in enumerate(self._tables):
            name = str(table.get("name", f"table_{idx}"))
            if name.lower() == table_name.lower():
                return table
        raise MDBReadError(f"Tabella {table_name!r} non trovata nel fallback meza")


def load_params(params_json: Optional[str], cli_overrides: Dict[str, object]) -> Dict[str, object]:
    params: Dict[str, object] = dict(DEFAULT_PARAMS)
    if params_json:
        with open(params_json, "r", encoding="utf-8") as f:
            payload = json.load(f)
        params.update(payload)

    for key, value in cli_overrides.items():
        if value is not None:
            params[key] = value

    y_var = str(params.get("YVarAlong", "Z")).upper()
    if y_var not in {"X", "Z"}:
        raise ValueError("YVarAlong deve essere 'X' oppure 'Z'")
    params["YVarAlong"] = y_var
    return params


def gather_input_files(files: Sequence[str], directory: Optional[str], recursive: bool) -> List[str]:
    collected: List[str] = []
    for item in files:
        collected.append(item)

    if directory:
        pattern = "**/*" if recursive else "*"
        for path in Path(directory).glob(pattern):
            if path.is_file() and path.suffix.lower() in {".mdb", ".accdb"}:
                collected.append(str(path))

    # dedup preservando ordine
    seen = set()
    unique: List[str] = []
    for p in collected:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


def open_mdb_reader(file_path: str, force_backend: str = "auto") -> BaseMDBReader:
    backend = force_backend.lower()
    if backend in {"auto", "pyodbc"}:
        try:
            return ODBCMDBReader(file_path)
        except MDBReadError:
            if backend == "pyodbc":
                raise
    if backend in {"auto", "meza"}:
        return MezaMDBReader(file_path)
    raise MDBReadError("Nessun backend disponibile (pyodbc/meza)")


def _is_numeric_column(type_name: Optional[str]) -> bool:
    if not type_name:
        return False
    t = type_name.lower()
    return any(token in t for token in ("int", "double", "float", "number", "numeric", "decimal", "real", "currency"))


def detect_xyz_table(
    reader: BaseMDBReader,
    preferred_tables: Sequence[str],
    aliases: Dict[str, Sequence[str]],
) -> Tuple[str, Tuple[str, str, str]]:
    tables = reader.list_tables()
    if not tables:
        raise MDBReadError("Nessuna tabella trovata nel database")

    preferred_map = {t.lower(): t for t in tables}
    for candidate in preferred_tables:
        if candidate.lower() in preferred_map:
            columns = reader.list_columns(preferred_map[candidate.lower()])
            xyz = detect_xyz_columns(columns, aliases)
            if xyz:
                return preferred_map[candidate.lower()], xyz

    for table_name in tables:
        columns = reader.list_columns(table_name)
        xyz = detect_xyz_columns(columns, aliases)
        if xyz:
            return table_name, xyz

    raise MDBReadError("Tabella con coordinate X,Y,Z non trovata")


def detect_xyz_columns(columns: Sequence[Tuple[str, Optional[str]]], aliases: Dict[str, Sequence[str]]) -> Optional[Tuple[str, str, str]]:
    lower_map = {name.lower(): name for name, _ in columns}

    def find_axis(axis: str) -> Optional[str]:
        for a in aliases[axis]:
            if a.lower() in lower_map:
                return lower_map[a.lower()]
        return None

    x_col = find_axis("x")
    y_col = find_axis("y")
    z_col = find_axis("z")
    if x_col and y_col and z_col:
        return x_col, y_col, z_col

    numeric_cols = [name for name, tname in columns if _is_numeric_column(tname)]
    if len(numeric_cols) >= 3:
        return numeric_cols[0], numeric_cols[1], numeric_cols[2]

    return None


def bbox(points: Sequence[Tuple[float, float, float]]) -> Tuple[float, float, float, float, float, float]:
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    zs = [p[2] for p in points]
    return min(xs), max(xs), min(ys), max(ys), min(zs), max(zs)


def analyze_points(
    file_path: str,
    table_name: str,
    points: Sequence[Tuple[float, float, float]],
    params: Dict[str, object],
) -> AnalysisResult:
    if not points:
        return AnalysisResult(
            file_path=file_path,
            table_name=table_name,
            point_count=0,
            status="FAIL",
            message="Nessun punto numerico valido trovato",
            violation=math.inf,
        )

    Xmin = float(params["Xmin"])
    Xmax = float(params["Xmax"])
    Ymin = float(params["Ymin"])
    Y1_max = float(params["Y1_max"])
    Y2_max = float(params["Y2_max"])
    Zmin = float(params["Zmin"])
    Zmax = float(params["Zmax"])
    m = float(params["OD"]) / 2.0 + float(params["clearance"])
    y_var = str(params["YVarAlong"])

    xmin, xmax, ymin, ymax, zmin, zmax = bbox(points)

    dx = (Xmin + m) - xmin
    dz = (Zmin + m) - zmin

    x_excess = (xmax + dx) - (Xmax - m)
    z_excess = (zmax + dz) - (Zmax - m)

    if x_excess > 0 or z_excess > 0:
        violation = max(0.0, x_excess, z_excess)
        return AnalysisResult(
            file_path=file_path,
            table_name=table_name,
            point_count=len(points),
            status="FAIL",
            message="Superati limiti X/Z anche dopo traslazione minima",
            dx=dx,
            dz=dz,
            violation=violation,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax,
            zmin=zmin,
            zmax=zmax,
        )

    dy_min = (Ymin + m) - ymin

    if y_var == "Z":
        denom = Zmax - Zmin
        t_values = [((z - Zmin) / denom) if denom != 0 else 0.0 for _, _, z in points]
    else:
        denom = Xmax - Xmin
        t_values = [((x - Xmin) / denom) if denom != 0 else 0.0 for x, _, _ in points]

    y_limits = [Y2_max + (Y1_max - Y2_max) * t for t in t_values]
    dy_candidates = [yl - m - y for (_, y, _), yl in zip(points, y_limits)]
    dy_max = min(dy_candidates)

    if dy_min > dy_max:
        violation = dy_min - dy_max
        return AnalysisResult(
            file_path=file_path,
            table_name=table_name,
            point_count=len(points),
            status="FAIL",
            message="Nessuna traslazione Y valida (dy_min > dy_max)",
            dx=dx,
            dy=dy_min,
            dz=dz,
            violation=violation,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax,
            zmin=zmin,
            zmax=zmax,
        )

    dy = dy_min

    return AnalysisResult(
        file_path=file_path,
        table_name=table_name,
        point_count=len(points),
        status="PASS",
        message="Percorso traslabile nel volume utile",
        dx=dx,
        dy=dy,
        dz=dz,
        violation=0.0,
        xmin=xmin,
        xmax=xmax,
        ymin=ymin,
        ymax=ymax,
        zmin=zmin,
        zmax=zmax,
        xmin_shifted=xmin + dx,
        xmax_shifted=xmax + dx,
        ymin_shifted=ymin + dy,
        ymax_shifted=ymax + dy,
        zmin_shifted=zmin + dz,
        zmax_shifted=zmax + dz,
    )


def analyze_file(
    file_path: str,
    params: Dict[str, object],
    preferred_tables: Sequence[str],
    aliases: Dict[str, Sequence[str]],
    backend: str,
) -> AnalysisResult:
    if not os.path.exists(file_path):
        return AnalysisResult(
            file_path=file_path,
            table_name=None,
            point_count=0,
            status="ERROR",
            message="File non esistente",
            violation=math.inf,
        )

    reader: Optional[BaseMDBReader] = None
    try:
        reader = open_mdb_reader(file_path, force_backend=backend)
        table_name, xyz_cols = detect_xyz_table(reader, preferred_tables, aliases)
        points = reader.fetch_columns(table_name, xyz_cols)
        result = analyze_points(file_path, table_name, points, params)
        if result.status == "PASS":
            result.message = f"PASS ({xyz_cols[0]}, {xyz_cols[1]}, {xyz_cols[2]})"
        return result
    except MDBReadError as exc:
        return AnalysisResult(
            file_path=file_path,
            table_name=None,
            point_count=0,
            status="ERROR",
            message=str(exc),
            violation=math.inf,
        )
    except Exception as exc:
        return AnalysisResult(
            file_path=file_path,
            table_name=None,
            point_count=0,
            status="ERROR",
            message=f"Errore inatteso: {exc}",
            violation=math.inf,
        )
    finally:
        if reader is not None:
            reader.close()


def print_result(result: AnalysisResult) -> None:
    print(f"File: {result.file_path}")
    print(f"  Stato: {result.status}")
    print(f"  Messaggio: {result.message}")
    print(f"  Tabella: {result.table_name}")
    print(f"  Punti: {result.point_count}")
    print(f"  BBox orig: X[{result.xmin:.3f}, {result.xmax:.3f}] "
          f"Y[{result.ymin:.3f}, {result.ymax:.3f}] "
          f"Z[{result.zmin:.3f}, {result.zmax:.3f}]")
    if result.status == "PASS":
        print(f"  Offset: dx={result.dx:.3f}, dy={result.dy:.3f}, dz={result.dz:.3f}")
        print(f"  BBox shift: X[{result.xmin_shifted:.3f}, {result.xmax_shifted:.3f}] "
              f"Y[{result.ymin_shifted:.3f}, {result.ymax_shifted:.3f}] "
              f"Z[{result.zmin_shifted:.3f}, {result.zmax_shifted:.3f}]")
    else:
        print(f"  Offset proposto: dx={result.dx:.3f}, dy={result.dy:.3f}, dz={result.dz:.3f}")
    print(f"  Violazione massima: {result.violation:.3f}")
    print()


def write_csv(path: str, results: Sequence[AnalysisResult]) -> None:
    fields = [
        "file_path", "table_name", "point_count", "status", "message",
        "dx", "dy", "dz", "violation",
        "xmin", "xmax", "ymin", "ymax", "zmin", "zmax",
        "xmin_shifted", "xmax_shifted", "ymin_shifted", "ymax_shifted", "zmin_shifted", "zmax_shifted",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            writer.writerow({k: getattr(r, k) for k in fields})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verifica ingombro percorsi da file .mdb/.accdb")
    parser.add_argument("files", nargs="*", help="Lista file .mdb/.accdb")
    parser.add_argument("--dir", help="Directory da cui leggere automaticamente i file")
    parser.add_argument("--recursive", action="store_true", help="Ricerca ricorsiva in --dir")
    parser.add_argument("--params-json", help="File JSON con parametri macchina")
    parser.add_argument("--csv-out", default="risultati_ingombro.csv", help="Percorso CSV output")
    parser.add_argument("--backend", choices=["auto", "pyodbc", "meza"], default="auto")
    parser.add_argument("--table", action="append", default=[], help="Nome tabella preferita (ripetibile)")

    # Override parametri macchina via CLI
    parser.add_argument("--Xmin", type=float)
    parser.add_argument("--Xmax", type=float)
    parser.add_argument("--Ymin", type=float)
    parser.add_argument("--Y1_max", type=float)
    parser.add_argument("--Y2_max", type=float)
    parser.add_argument("--Zmin", type=float)
    parser.add_argument("--Zmax", type=float)
    parser.add_argument("--OD", type=float)
    parser.add_argument("--clearance", type=float)
    parser.add_argument("--YVarAlong", choices=["X", "Z", "x", "z"])
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    files = gather_input_files(args.files, args.dir, args.recursive)
    if not files:
        parser.error("Specificare almeno un file oppure --dir")

    overrides = {
        "Xmin": args.Xmin,
        "Xmax": args.Xmax,
        "Ymin": args.Ymin,
        "Y1_max": args.Y1_max,
        "Y2_max": args.Y2_max,
        "Zmin": args.Zmin,
        "Zmax": args.Zmax,
        "OD": args.OD,
        "clearance": args.clearance,
        "YVarAlong": args.YVarAlong.upper() if args.YVarAlong else None,
    }
    params = load_params(args.params_json, overrides)

    aliases = {
        "x": ("X", "x"),
        "y": ("Y", "y"),
        "z": ("Z", "z"),
    }

    results: List[AnalysisResult] = []
    for file_path in files:
        result = analyze_file(
            file_path=file_path,
            params=params,
            preferred_tables=args.table,
            aliases=aliases,
            backend=args.backend,
        )
        print_result(result)
        results.append(result)

    write_csv(args.csv_out, results)
    print(f"CSV salvato in: {args.csv_out}")

    return 0 if all(r.status == "PASS" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
