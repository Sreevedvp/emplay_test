import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  input,
  signal,
} from '@angular/core';
import { DatePipe, DecimalPipe } from '@angular/common';
import { ScrollingModule } from '@angular/cdk/scrolling';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { toSignal } from '@angular/core/rxjs-interop';
import { FleetStore } from '../core/fleet.store';
import { Service, CONFIG } from '../core/models';
@Component({
  selector: 'app-fleet-row',
  standalone: true,
  imports: [RouterLink, DecimalPipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { role: 'row', '[class.stale-row]': 'stale()' },
  template: ` <span role="cell"
      ><a [routerLink]="['/services', service().id]">{{ service().name }}</a></span
    ><span role="cell">{{ service().team }}</span>
    <span role="cell">{{ metric()?.p95 | number: '1.0-1' }}</span
    ><span role="cell">{{ metric()?.errRate | number: '1.0-2' }}</span
    ><span role="cell">{{ metric()?.rps | number: '1.0-0' }}</span>
    <span role="cell" [class.flash]="flashing()"
      >{{ metric()?.status ?? 'unknown' }}{{ stale() ? ' · stale' : '' }}</span
    >`,
})
export class FleetRow {
  service = input.required<Service>();
  private store = inject(FleetStore);
  metric = computed(() => this.store.row(this.service().id)());
  stale = computed(() => this.store.clock() - (this.metric()?.ts ?? 0) > CONFIG.staleMs);
  flashing = signal(false);
  private status = computed(() => this.metric()?.status ?? '');
  constructor() {
    let previous = '';
    effect((onCleanup) => {
      const status = this.status();
      if (previous && previous !== status) {
        this.flashing.set(true);
        const timer = setTimeout(() => this.flashing.set(false), 800);
        onCleanup(() => clearTimeout(timer));
      }
      previous = status;
    });
  }
}
@Component({
  selector: 'app-fleet',
  standalone: true,
  imports: [ScrollingModule, FleetRow, DatePipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <h1>Live fleet</h1>
    <div class="controls">
      <label
        >Search <input type="search" [value]="query().search" (input)="control('q', $event)"
      /></label>
      <label
        >Status
        <select [value]="query().status" (change)="control('status', $event)">
          <option value="">All</option>
          <option>healthy</option>
          <option>degraded</option>
          <option>down</option>
        </select></label
      >
      <label
        >Sort
        <select [value]="query().sort" (change)="control('sort', $event)">
          <option value="name">Name</option>
          <option value="p95">Latency</option>
          <option value="errRate">Error rate</option>
          <option value="rps">Requests/s</option>
        </select></label
      >
      <button type="button" (click)="set('dir', query().direction === 'asc' ? 'desc' : 'asc')">
        {{ query().direction === 'asc' ? 'Ascending ↑' : 'Descending ↓' }}
      </button>
    </div>
    <p>{{ visible().length }} services · Only visible rows are rendered.</p>
    @if (store.error()) {
      <p role="alert">{{ store.error() }} <button (click)="store.retry()">Retry fleet</button></p>
    }
    <div
      role="table"
      aria-label="Live microservice metrics"
      [attr.aria-rowcount]="visible().length + 1"
    >
      <div role="row" class="table-header">
        <span role="columnheader">Service</span><span role="columnheader">Team</span
        ><span role="columnheader">p95 (ms)</span><span role="columnheader">Errors (%)</span
        ><span role="columnheader">Requests/s</span><span role="columnheader">Status</span>
      </div>
      <cdk-virtual-scroll-viewport
        itemSize="40"
        minBufferPx="120"
        maxBufferPx="240"
        tabindex="0"
        aria-label="Scrollable service rows"
      >
        <app-fleet-row
          *cdkVirtualFor="let service of visible(); trackBy: track"
          [service]="service"
        />
      </cdk-virtual-scroll-viewport>
    </div>
    <h2>Incidents</h2>
    <p>
      Most recent {{ store.incidents().length }} (maximum 200). One notification per sustained rule
      episode.
    </p>
    <ul>
      @for (incident of store.incidents(); track incident.id) {
        <li>
          {{ incident.ts | date: 'mediumTime' }} · {{ incident.rule }} · {{ incident.serviceId }}
        </li>
      } @empty {
        <li>No incidents observed in this session.</li>
      }
    </ul>`,
})
export class Fleet {
  store = inject(FleetStore);
  private router = inject(Router);
  private route = inject(ActivatedRoute);
  private params = toSignal(this.route.queryParamMap, {
    initialValue: this.route.snapshot.queryParamMap,
  });
  query = computed(() => {
    const p = this.params();
    return {
      search: p.get('q') ?? '',
      status: p.get('status') ?? '',
      sort: p.get('sort') ?? 'name',
      direction: p.get('dir') === 'desc' ? 'desc' : 'asc',
    };
  });
  visible = computed(() => {
    const q = this.query();
    if (q.status || ['p95', 'errRate', 'rps'].includes(q.sort)) this.store.revision();
    const search = q.search.toLowerCase();
    return this.store
      .services()
      .filter(
        (s) =>
          (!search || `${s.id} ${s.name} ${s.team}`.toLowerCase().includes(search)) &&
          (!q.status || this.store.latest.get(s.id)?.status === q.status),
      )
      .sort((a, b) => {
        const metric = q.sort === 'p95' || q.sort === 'errRate' || q.sort === 'rps' ? q.sort : null;
        const comparison = metric
          ? (this.store.latest.get(a.id)?.[metric] ?? 0) -
            (this.store.latest.get(b.id)?.[metric] ?? 0)
          : a.name.localeCompare(b.name);
        return (comparison || a.id.localeCompare(b.id)) * (q.direction === 'asc' ? 1 : -1);
      });
  });
  track(_index: number, service: Service): string {
    return service.id;
  }
  control(key: string, event: Event): void {
    this.set(key, (event.target as HTMLInputElement | HTMLSelectElement).value);
  }
  set(key: string, value: string): void {
    void this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { [key]: value || null },
      queryParamsHandling: 'merge',
      replaceUrl: true,
    });
  }
}
