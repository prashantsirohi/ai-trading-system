import { NavLink } from 'react-router-dom';
import {
  BeakerIcon,
  BookmarkIcon,
  ChartBarIcon,
  CpuChipIcon,
  HomeIcon,
  PlayIcon,
  PresentationChartLineIcon,
  RocketLaunchIcon,
  ShieldExclamationIcon,
  Squares2X2Icon,
  ViewColumnsIcon,
} from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils/cn';

const items = [
  { to: '/', label: 'Control Tower', icon: HomeIcon, end: true },
  { to: '/pipeline', label: 'Pipeline', icon: ViewColumnsIcon, end: false },
  { to: '/ranking', label: 'Ranking', icon: ChartBarIcon, end: false },
  { to: '/watchlist', label: 'Watchlist', icon: BookmarkIcon, end: false },
  { to: '/patterns', label: 'Patterns', icon: PresentationChartLineIcon, end: false },
  { to: '/sectors', label: 'Sectors', icon: Squares2X2Icon, end: false },
  { to: '/execution', label: 'Execution', icon: RocketLaunchIcon, end: false },
  { to: '/risk', label: 'Risk', icon: ShieldExclamationIcon, end: false },
  { to: '/runs', label: 'Runs', icon: PlayIcon, end: false },
  { to: '/shadow', label: 'Shadow', icon: CpuChipIcon, end: false },
  { to: '/research', label: 'Research', icon: BeakerIcon, end: false },
];

export default function Sidebar() {
  return (
    <aside className="hidden w-72 border-r border-slate-800 bg-slate-950 p-4 lg:block">
      <div className="mb-6 rounded-3xl border border-slate-800 bg-slate-900 p-4 shadow-soft">
        <div className="text-sm text-slate-400">AI Trading System</div>
        <div className="mt-1 text-xl font-semibold">Operator Console</div>
      </div>

      <nav className="space-y-2">
        {items.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-2xl border px-4 py-3 transition',
                  isActive
                    ? 'border-blue-500/40 bg-blue-500/10 text-white'
                    : 'border-slate-800 bg-slate-900/50 text-slate-300 hover:bg-slate-900 hover:text-white',
                )
              }
            >
              <Icon className="h-5 w-5" />
              <span>{item.label}</span>
            </NavLink>
          );
        })}
      </nav>
    </aside>
  );
}
