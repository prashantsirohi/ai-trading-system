/**
 * Artifacts list for the active run, grouped by stage.
 *
 * Each row exposes the gated download URL (``/api/execution/artifacts/{run_id}/{stage}/{name}``)
 * so the operator can pull raw files. Rows are clickable as ``<a>`` to lean
 * on the browser's native auth/cookie handling for the dev proxy.
 */
import type { ArtifactRecord, RunArtifacts } from '@/lib/api/runs';
import { API_BASE_URL } from '@/lib/api/client';

interface Props {
  data: RunArtifacts | null | undefined;
  isLoading: boolean;
}

function groupByStage(artifacts: ArtifactRecord[]): Map<string, ArtifactRecord[]> {
  const map = new Map<string, ArtifactRecord[]>();
  for (const a of artifacts) {
    const existing = map.get(a.stageName) ?? [];
    existing.push(a);
    map.set(a.stageName, existing);
  }
  return map;
}

function shortBytes(rowCount: number | null): string {
  if (rowCount === null) return '—';
  return `${rowCount.toLocaleString()} rows`;
}

export default function ArtifactsList({ data, isLoading }: Props) {
  if (isLoading) {
    return <p className="text-xs text-slate-500">Loading artifacts…</p>;
  }
  if (!data || !data.available) {
    return <p className="text-xs text-slate-500">No artifacts registered for this run.</p>;
  }
  if (data.artifacts.length === 0) {
    return <p className="text-xs text-slate-500">Run produced no artifacts.</p>;
  }

  const grouped = groupByStage(data.artifacts);

  return (
    <div className="space-y-4">
      {Array.from(grouped.entries()).map(([stage, items]) => (
        <div key={stage}>
          <p className="mb-1 text-[11px] font-semibold uppercase tracking-widest text-slate-500">
            {stage} · {items.length}
          </p>
          <ul className="space-y-1">
            {items.map((a) => {
              const href = API_BASE_URL ? `${API_BASE_URL}${a.downloadUrl}` : a.downloadUrl;
              return (
                <li
                  key={a.artifactId}
                  className="flex items-center gap-2 rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2 text-xs"
                >
                  <span className="font-mono text-slate-200">{a.name}</span>
                  <span className="rounded-full border border-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-400">
                    {a.artifactType}
                  </span>
                  <span className="ml-auto tabular-nums text-slate-400">
                    {shortBytes(a.rowCount)}
                  </span>
                  <a
                    href={href}
                    download={a.name}
                    className="rounded-full border border-blue-500/40 bg-blue-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-blue-200 transition-colors hover:border-blue-300/60"
                  >
                    Download
                  </a>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </div>
  );
}
