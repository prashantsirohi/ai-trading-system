import { NavLink, useLocation } from 'react-router-dom';
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

const primaryItems = [
  { to: '/', label: 'Control Tower', icon: HomeIcon, end: true, match: ['/'] },
  { to: '/pipeline', label: 'Pipeline', icon: ViewColumnsIcon, end: false, match: ['/pipeline'] },
  { to: '/ranking', label: 'Ranking', icon: ChartBarIcon, end: false, match: ['/ranking', '/symbol'] },
  { to: '/watchlist', label: 'Watchlist', icon: BookmarkIcon, end: false, match: ['/watchlist'] },
  { to: '/patterns', label: 'Patterns', icon: PresentationChartLineIcon, end: false, match: ['/patterns'] },
  { to: '/sectors', label: 'Sectors', icon: Squares2X2Icon, end: false, match: ['/sectors'] },
  { to: '/execution', label: 'Execution', icon: RocketLaunchIcon, end: false, match: ['/execution'] },
  { to: '/risk', label: 'Risk', icon: ShieldExclamationIcon, end: false, match: ['/risk'] },
  { to: '/runs', label: 'Runs', icon: PlayIcon, end: false, match: ['/runs'] },
];

const secondaryItems = [
  { to: '/shadow', label: 'Shadow', icon: CpuChipIcon, end: false, match: ['/shadow'] },
  { to: '/research', label: 'Research', icon: BeakerIcon, end: false, match: ['/research'] },
];

type NavItem = (typeof primaryItems)[number];

function itemIsActive(pathname: string, item: NavItem): boolean {
  if (item.end) return pathname === item.to;
  return item.match.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
}

function ModuleLink({ item, compact = false }: { item: NavItem; compact?: boolean }) {
  const Icon = item.icon;
  return (
    <NavLink
      to={item.to}
      end={item.end}
      title={item.label}
      aria-label={item.label}
      className={({ isActive }) =>
        cn(
          'inline-flex h-9 shrink-0 items-center gap-2 rounded-xl border px-3 text-sm font-medium transition',
          isActive
            ? 'border-blue-500/50 bg-blue-500/15 text-white'
            : 'border-slate-800 bg-slate-900/55 text-slate-300 hover:border-slate-700 hover:bg-slate-900 hover:text-white',
          compact && 'w-full justify-start',
        )
      }
    >
      <Icon className="h-4 w-4" />
      <span className={cn(!compact && 'hidden sm:inline')}>{item.label}</span>
    </NavLink>
  );
}

export default function ModuleNav() {
  const { pathname } = useLocation();
  const moreActive = secondaryItems.some((item) => itemIsActive(pathname, item));

  return (
    <div className="border-b border-slate-800 bg-slate-950/95 px-4 py-2 md:px-6">
      <nav className="flex min-w-0 items-center gap-2" aria-label="Primary modules">
        <div className="mr-2 hidden shrink-0 items-center border-r border-slate-800 pr-4 xl:flex">
          <div>
            <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">AI Trading System</div>
            <div className="text-sm font-semibold text-slate-200">Operator Console</div>
          </div>
        </div>

        <div className="flex min-w-0 flex-1 items-center gap-2 overflow-x-auto pb-1">
          {primaryItems.map((item) => (
            <ModuleLink key={item.to} item={item} />
          ))}
        </div>

        <details className="relative shrink-0">
          <summary
            className={cn(
              'flex h-9 cursor-pointer list-none items-center gap-2 rounded-xl border px-3 text-sm font-medium transition marker:hidden',
              moreActive
                ? 'border-blue-500/50 bg-blue-500/15 text-white'
                : 'border-slate-800 bg-slate-900/55 text-slate-300 hover:border-slate-700 hover:bg-slate-900 hover:text-white',
            )}
          >
            More
          </summary>
          <div className="absolute right-0 z-40 mt-2 w-44 rounded-xl border border-slate-800 bg-slate-950 p-2 shadow-2xl">
            <div className="flex flex-col gap-1">
              {secondaryItems.map((item) => (
                <ModuleLink key={item.to} item={item} compact />
              ))}
            </div>
          </div>
        </details>
      </nav>
    </div>
  );
}
