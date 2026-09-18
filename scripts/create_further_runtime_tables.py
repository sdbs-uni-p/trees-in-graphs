#!/usr/bin/env python3
"""Reproduce the complete supplementary tables from the bundled inputs.

Paper rows take precedence. The 6 h timeout interpretation is a user-confirmed
correction recorded in inputs.json, not an inference from historical error logs.
"""

import csv
import hashlib
import json
import os
import shutil
import sys
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

from create_combined_overview import (
    compact_graph, compact_rows, content_widths, rounded_rows, selected_ldbc_rows,
)
from create_combined_runtime_tables import (
    METHODS, SYSTEMS, graph_label, graph_sort_key, load_ldbc_medians,
    render_pdf, split_graph, svg_page,
)
from create_runtime_tables import (
    SCENARIO_ORDER, Timeout, median_runtime,
    runtime_speedup, runtime_text, write_sources,
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'results/combined/further_results'
SYSTEM_KEYS = ('age', 'kuzu', 'neo4j')
QUERY_LABELS = {
    '01_all_descendants': 'Q_desc',
    '02_all_children': 'Q_child',
    '05_all_leaves': 'Q_leaf',
    '11_check_if_ancestor': 'Q_a&d',
}
TIMEOUT_MS = 6 * 60 * 60 * 1000
FIELDS = ['graph', 'query', 'scenario', 'run', 'runtime_ms']
PARAMETER_LABELS = {
    'q01_q02': 'Root', 'q01': 'Root', 'q02': 'Deep root',
    'q03': 'Leaf parent H', 'q04': 'Leaf parent', 'q03_q04': 'Leaf parent H/L',
    'q05': 'High degree', 'q06': 'Low degree',
    'q07': 'Root–leaf', 'q08': 'Deep parent–leaf',
    'q09': 'Siblings', 'q10': 'Distant leaves',
}
PARAMETER_NOTES = {
    'Q_desc': (
        'Parameters: Root = root of largest tree (also deepest when identical); Deep root = root of deepest tree.',
        'Leaf parent = low-degree leaf parent; Leaf parent H = high-degree leaf parent; H/L = identical high/low-degree cases.',
    ),
    'Q_child': (
        'Parameters: High degree = highest-degree node; Low degree = lowest-degree node.',
    ),
    'Q_a&d': (
        'Parameters: Root–leaf = root-farthest-leaf pair; Deep parent–leaf = deepest parent-leaf pair (both true).',
        'Siblings = shallow sibling pair; Distant leaves = leaf pair with greatest distance via their lowest common ancestor (both false).',
    ),
    'LDBC': (),
}
PARAMETER_NOTES['Q_leaf'] = PARAMETER_NOTES['Q_desc']


def read_csv(path):
    with path.open(newline='', encoding='utf-8-sig') as handle:
        return list(csv.DictReader(handle))


def fingerprint(path, directory=OUTPUT):
    return {
        'path': Path(os.path.relpath(path, directory)).as_posix(),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')


def collect_inputs(expected, input_directory, output_directory):
    """Validate the frozen package before copying it or interpreting any blanks."""
    manifest = json.loads((input_directory / 'inputs.json').read_text())
    if manifest.get('schema_version') != 1:
        raise ValueError('Unsupported further-results manifest')
    inputs, medians, source_paths = {}, {}, {}
    for system in (*SYSTEM_KEYS, *(name + '_ldbc' for name in SYSTEM_KEYS)):
        entry = manifest['inputs'][system]
        relative = Path('inputs') / system / 'runtimes.csv'
        if entry['csv']['path'] != relative.as_posix():
            raise ValueError(f'Expected bundled input {relative}')
        source = input_directory / relative
        if hashlib.sha256(source.read_bytes()).hexdigest() != entry['csv']['sha256']:
            raise ValueError(f'Frozen input checksum mismatch: {relative}')
        source_paths[system] = source
    for system, display in zip(SYSTEM_KEYS, SYSTEMS):
        rows = read_csv(source_paths[system])
        keys = [tuple(row[field] for field in FIELDS[:-1]) for row in rows]
        expected_keys = {
            (graph + '_' + method, query, scenario, str(run))
            for graph, query, scenario in expected
            for method in METHODS for run in range(1, 6)
        }
        if len(keys) != len(set(keys)) or set(keys) != expected_keys:
            raise ValueError(f'Incomplete or duplicate frozen measurements: {system}')
        timeout_rows = {}
        for group in manifest['inputs'][system].get('timeout_groups', []):
            if group['timeout_ms'] != TIMEOUT_MS:
                raise ValueError('The frozen further-results package requires the confirmed 6 h limit')
            for run in group['runs']:
                key = (group['graph'] + '_' + group['method'], group['query'], group['scenario'], str(run))
                timeout_rows[key] = group['timeout_ms']
        blanks = {tuple(row[field] for field in FIELDS[:-1]) for row in rows if not row['runtime_ms'].strip()}
        if blanks != set(timeout_rows):
            raise ValueError(f'Missing measurements do not match the declared timeout rows: {system}')
        groups = defaultdict(list)
        for row in rows:
            graph, method = split_graph(row['graph'])
            key = (graph, row['query'], row['scenario'], method)
            if row['runtime_ms'].strip():
                value = float(row['runtime_ms'])
                if not 0 < value < float('inf'):
                    raise ValueError(f'Invalid runtime: {row}')
            else:
                value = Timeout(timeout_rows[tuple(row[field] for field in FIELDS[:-1])])
            groups[key].append(value)
        medians[display] = {key: median_runtime(values, key) for key, values in groups.items()}
    for system, source in source_paths.items():
        destination = output_directory / 'inputs' / system / 'runtimes.csv'
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.resolve() != destination.resolve():
            shutil.copyfile(source, destination)
        inputs[system] = destination
    write_json(output_directory / 'inputs.json', manifest)
    return inputs, medians


def tree_rows(medians, expected):
    result = []
    for graph, query, scenario in sorted(expected, key=lambda key: (
            key[1], graph_sort_key(key[0]), SCENARIO_ORDER[key[2]])):
        label = PARAMETER_LABELS[scenario]
        row = [compact_graph(graph_label(graph)), QUERY_LABELS[query], label]
        for system in SYSTEMS:
            times = {method: medians[system][(graph, query, scenario, method)] for method in METHODS}
            row.extend(runtime_text(times[method]) for method in METHODS)
            row.extend(runtime_speedup(times['baseline'], times[method]) for method in METHODS[1:])
        result.append(row)
    return result


def page_groups(rows):
    """Split each Tree query after DT100K and SNB/T; keep LDBC separate."""
    pages = []
    for query in (*QUERY_LABELS.values(), 'LDBC'):
        selected = [row for row in rows if (row[1] == query if query != 'LDBC' else row[1] in ('IC12', 'IS2', 'IS6'))]
        current = []
        for graph in dict.fromkeys(row[0] for row in selected):
            group = [row for row in selected if row[0] == graph]
            current.extend(group)
            if graph in ('DT100K', 'SNB/T'):
                pages.append((query, current))
                current = []
        if current:
            pages.append((query, current))
    return pages


def generate(output_paths, input_directory=OUTPUT):
    by_directory = defaultdict(dict)
    for variant, path in output_paths.items():
        by_directory[path.resolve().parent][variant] = path.resolve()
    if len(by_directory) > 1:
        for paths in by_directory.values():
            generate(paths, input_directory)
        return
    output_directory, output_paths = next(iter(by_directory.items()))
    output_directory.mkdir(parents=True, exist_ok=True)
    expected = {(r['graph'], r['query'], r['scenario']) for r in read_csv(ROOT / 'experiments/query_parameters.csv')}
    assert len(expected) == 240
    inputs, medians = collect_inputs(expected, input_directory, output_directory)
    exact = tree_rows(medians, expected)
    ldbc = {display: load_ldbc_medians(inputs[system + '_ldbc']) for system, display in zip(SYSTEM_KEYS, SYSTEMS)}
    exact.extend(selected_ldbc_rows(ldbc))
    assert len(exact) == 243
    assert sum(row[3] == '>6 h' for row in exact) == 5
    assert sum(value.startswith('>') and value.endswith('x') for row in exact for value in row) == 10
    rounded = rounded_rows(exact)
    outputs = []
    titles = {
        'Q_desc': 'All Descendants (Query 01)', 'Q_child': 'All Children (Query 02)',
        'Q_leaf': 'All Leaves (Query 05)', 'Q_a&d': 'Check if Ancestor (Query 11)',
        'LDBC': 'LDBC SNB SF1: IC12, IS2, IS6',
    }
    with tempfile.TemporaryDirectory(prefix='further-results-') as temporary:
        temp = Path(temporary)
        for variant, rows, headings in (
            ('exact', exact, ('B (ms)', 'D', 'P', 'S_D', 'S_P')),
            ('rounded', rounded, ('B (ms)', 'D', 'P', 'S_D', 'S_P')),
            ('compact', compact_rows(rounded), ('B (ms)', 'S_D', 'S_P')),
        ):
            if variant not in output_paths:
                continue
            leading, metrics = content_widths(rows, headings)
            # Use the paper's widths; reserve extra space only for the bold
            # bound itself, not for all ordinary values in a timeout column.
            for column in range(len(metrics)):
                bounds = [row[3 + column] for row in rows
                          if row[3 + column].startswith(('>', '<'))]
                if bounds:
                    metrics[column] = max(metrics[column], max(
                        len(value) * 6.1 + 6 for value in bounds
                    ))
            pdfs = []
            for number, (query, page_rows) in enumerate(page_groups(rows), 1):
                notes = (
                    ('Queries: IC12 = Interactive Complex 12; IS2 = Interactive Short 2; IS6 = Interactive Short 6.'
                     if query == 'LDBC' else
                     'Queries: Q_desc = All Descendants; Q_child = All Children; Q_leaf = All Leaves; Q_a&d = Check if Ancestor.'),
                    *PARAMETER_NOTES[query],
                    'Methods: B = Baseline; D = Dewey; P = PrePost; S_D = B / D; S_P = B / P. Runtimes: median of five runs in ms.',
                    'Graphs: F = forest; NT = truebase; DT = ultratall; WT = ultrawide; K = 1,000 nodes.',
                    'SNB/C, SNB/P, SNB/T = SNB SF1 trees; SNB = full SNB SF1 graph.',
                )
                if variant != 'exact':
                    notes += ('Rounding: runtimes to one decimal; speedups to about two significant digits; K = 1,000x. Bounds round outward.',)
                svg = svg_page(
                    titles[query], page_rows,
                    leading_headings=('Graph', 'Query', 'Parameters'),
                    leading_widths=leading, metric_widths=metrics, subheadings=headings,
                    graph_notes=False, notes_override=notes,
                    row_height=12.3, header_height=15, subheader_height=14.5, body_font_size=9.1,
                    table_only=variant == 'compact', fit_content=variant != 'compact',
                )
                # These limits are user-confirmed overrides, not historical log claims.
                svg = svg.replace('logged limit T', 'user-confirmed limit T')
                svg_path = temp / f'{variant}-{number}.svg'
                pdf_path = temp / f'{variant}-{number}.pdf'
                svg_path.write_text(svg)
                render_pdf(svg_path, pdf_path)
                pdfs.append(pdf_path)
            output = output_paths[variant]
            subprocess.run(['pdfunite', *map(str, pdfs), str(output)], check=True)
            outputs.append(output)
            print(f'{output}: {len(pdfs)} pages, {len(rows)} rows')
    write_sources(inputs, outputs, {})
    sources_path = output_directory / 'sources.json'
    sources = json.loads(sources_path.read_text())
    for output in outputs:
        report = sources['reports'][output.name]
        report['selection_manifest'] = fingerprint(output_directory / 'inputs.json', output_directory)
        report['generator'] = {'name': Path(__file__).name, 'sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        report['timeout_policy'] = 'User-confirmed 21600000 ms for all 25 missing AGE measurements; see inputs.json'
    write_json(sources_path, sources)



if __name__ == '__main__':
    from create_combined_overview import main
    main(['--results', 'further', *sys.argv[1:]])
