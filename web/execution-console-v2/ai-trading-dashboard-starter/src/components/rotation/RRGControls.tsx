import type { RRGLabelMode, RRGScaleMode } from './RRGChart';

type Props = {
  groupType: 'sector' | 'industry';
  onGroupTypeChange: (value: 'sector' | 'industry') => void;
  availableDates: string[];
  selectedDate: string | null;
  onDateChange: (value: string | null) => void;
  isPlaying: boolean;
  onPlayingChange: (value: boolean) => void;
  tailLength: number;
  onTailLengthChange: (value: number) => void;
  scaleMode: RRGScaleMode;
  onScaleModeChange: (value: RRGScaleMode) => void;
  sectors: string[];
  selectedSector: string | null;
  onSectorChange: (value: string | null) => void;
  search: string;
  onSearchChange: (value: string) => void;
  labelMode: RRGLabelMode;
  onLabelModeChange: (value: RRGLabelMode) => void;
  onFullView: () => void;
};

export default function RRGControls({
  groupType,
  onGroupTypeChange,
  availableDates,
  selectedDate,
  onDateChange,
  isPlaying,
  onPlayingChange,
  tailLength,
  onTailLengthChange,
  scaleMode,
  onScaleModeChange,
  sectors,
  selectedSector,
  onSectorChange,
  search,
  onSearchChange,
  labelMode,
  onLabelModeChange,
  onFullView,
}: Props) {
  const selectedIndex = Math.max(0, availableDates.indexOf(selectedDate ?? ''));

  return (
    <div className="grid gap-3 border-b border-slate-800 pb-3 xl:grid-cols-[auto_minmax(260px,1fr)_auto] xl:items-end">
      <div className="flex rounded-md border border-slate-700 bg-slate-950 p-1" aria-label="View">
        {(['industry', 'sector'] as const).map((value) => (
          <button
            key={value}
            type="button"
            className={`rounded-md px-3 py-2 text-sm font-medium ${groupType === value ? 'bg-slate-100 text-slate-950' : 'text-slate-300 hover:bg-slate-800'}`}
            onClick={() => onGroupTypeChange(value)}
          >
            {value === 'industry' ? 'Industry' : 'Sector'}
          </button>
        ))}
      </div>

      <div className="grid min-w-0 gap-2">
        <div className="flex items-center justify-between gap-3 text-xs text-slate-400">
          <span>{selectedDate ?? 'Latest date'}</span>
          <button
            type="button"
            className="rounded-md border border-slate-700 px-3 py-1 text-slate-200 hover:bg-slate-800"
            onClick={() => onPlayingChange(!isPlaying)}
          >
            {isPlaying ? 'Pause' : 'Play'}
          </button>
        </div>
        <input
          aria-label="Rotation date"
          type="range"
          min={0}
          max={Math.max(0, availableDates.length - 1)}
          value={selectedIndex}
          disabled={availableDates.length === 0}
          onChange={(event) => onDateChange(availableDates[Number(event.target.value)] ?? null)}
          className="w-full accent-sky-400"
        />
      </div>

      <div className="grid grid-cols-2 gap-2 md:grid-cols-3 xl:grid-cols-[110px_130px_170px_160px_130px_110px]">
        <select
          aria-label="Tail length"
          value={tailLength}
          onChange={(event) => onTailLengthChange(Number(event.target.value))}
          className="rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
        >
          {[5, 10, 20, 40, 80, 200].map((value) => (
            <option key={value} value={value}>{value}D tail</option>
          ))}
        </select>
        <select
          aria-label="Scale mode"
          value={scaleMode}
          onChange={(event) => onScaleModeChange(event.target.value as RRGScaleMode)}
          className="rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
        >
          <option value="focused">Focused 95-110</option>
          <option value="auto">Auto</option>
          <option value="wide">Wide 80-130</option>
        </select>
        <select
          aria-label="Sector filter"
          value={selectedSector ?? ''}
          onChange={(event) => onSectorChange(event.target.value || null)}
          className="rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
        >
          <option value="">All sectors</option>
          {sectors.map((sector) => (
            <option key={sector} value={sector}>{sector}</option>
          ))}
        </select>
        <input
          aria-label="Search group"
          value={search}
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder="Search"
          className="rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500"
        />
        <select
          aria-label="Label mode"
          value={labelMode}
          onChange={(event) => onLabelModeChange(event.target.value as RRGLabelMode)}
          className="rounded-md border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
        >
          <option value="top">Top 20 labels</option>
          <option value="selected">Selected only</option>
          <option value="all">All labels</option>
          <option value="off">Labels off</option>
        </select>
        <button
          type="button"
          className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800"
          onClick={onFullView}
        >
          Full view
        </button>
      </div>
    </div>
  );
}
