import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  OnDestroy,
  afterNextRender,
  computed,
  effect,
  inject,
  input,
  signal,
  viewChild,
} from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { toSignal } from '@angular/core/rxjs-interop';
import { Chart, LineController, LineElement, PointElement, LinearScale, Tooltip } from 'chart.js';
import { Auth } from '../core/auth.service';
import { FleetStore } from '../core/fleet.store';
import { CONFIG, Metric, Point } from '../core/models';
import { reducePoints } from '../core/algorithms';
Chart.register(LineController, LineElement, PointElement, LinearScale, Tooltip);
@Component({
  selector: 'app-metric-chart',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <section>
    <h2>{{ title() }}</h2>
    @if (error()) {
      <p role="alert">Chart failed: {{ error() }} <button (click)="retry()">Retry chart</button></p>
    }
    <div class="chart">
      <canvas #canvas role="img" [attr.aria-label]="title() + ': last 15 minutes'"></canvas>
    </div>
    <p>{{ points().length }} plotted points · Latest: {{ latest() }}</p>
  </section>`,
})
export class MetricChart implements OnDestroy {
  points = input.required<Point[]>();
  metric = input.required<Metric>();
  title = input.required<string>();
  canvas = viewChild.required<ElementRef<HTMLCanvasElement>>('canvas');
  error = signal('');
  ready = signal(false);
  private chart?: Chart<'line'>;
  latest = computed(() => {
    const points = this.points();
    return points.length ? points[points.length - 1][this.metric()].toFixed(2) : 'unavailable';
  });
  constructor() {
    afterNextRender(() => this.ready.set(true));
    effect(() => {
      if (this.ready()) this.draw(this.points());
    });
  }
  retry(): void {
    this.error.set('');
    this.chart?.destroy();
    this.chart = undefined;
    this.draw(this.points());
  }
  private draw(points: Point[]): void {
    if (this.error()) return;
    try {
      const data = points.map((p) => ({ x: p.ts, y: p[this.metric()] }));
      if (data.some((p) => !Number.isFinite(p.x) || !Number.isFinite(p.y)))
        throw new Error('Nonfinite chart data');
      if (!this.chart)
        this.chart = new Chart(this.canvas().nativeElement, {
          type: 'line',
          data: {
            datasets: [
              {
                data,
                borderColor: '#246d91',
                borderWidth: 1.5,
                pointRadius: 0,
                label: this.title(),
              },
            ],
          },
          options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            parsing: false,
            normalized: true,
            scales: {
              x: {
                type: 'linear',
                ticks: {
                  maxTicksLimit: 5,
                  callback: (value) => new Date(Number(value)).toLocaleTimeString(),
                },
              },
              y: { beginAtZero: true },
            },
          },
        });
      else {
        this.chart.data.datasets[0].data = data;
        this.chart.update('none');
      }
    } catch (e) {
      this.error.set(String(e));
      this.chart?.destroy();
      this.chart = undefined;
    }
  }
  ngOnDestroy(): void {
    this.chart?.destroy();
  }
}
@Component({
  selector: 'app-detail',
  standalone: true,
  imports: [RouterLink, MetricChart],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <a routerLink="/fleet">← Fleet</a>
    <h1>{{ id() }}</h1>
    <label
      >Switch service
      <select [value]="id()" (change)="switchService($event)">
        @for (s of store.services(); track s.id) {
          <option [value]="s.id" [selected]="s.id === id()">{{ s.name }}</option>
        }
      </select></label
    >
    @if (loading()) {
      <p role="status">Loading history…</p>
    }
    @if (error()) {
      <p role="alert">
        {{ error() }} <button (click)="reload.update(increment)">Retry history</button>
      </p>
    }
    @if (stale()) {
      <p role="status">This service's data is stale. Last known values remain visible.</p>
    }
    @if (!loading() && !error()) {
      <app-metric-chart
        [points]="points()"
        metric="p95"
        title="p95 latency (ms)"
      /><app-metric-chart
        [points]="points()"
        metric="errRate"
        title="Error rate (%)"
      /><app-metric-chart [points]="points()" metric="rps" title="Throughput (requests/s)" />
    }`,
})
export class Detail {
  private auth = inject(Auth);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  store = inject(FleetStore);
  private params = toSignal(this.route.paramMap, { initialValue: this.route.snapshot.paramMap });
  id = computed(() => this.params().get('id') ?? '');
  private chartData = signal<{ id: string; points: Point[] }>({ id: '', points: [] });
  points = computed(() => (this.chartData().id === this.id() ? this.chartData().points : []));
  loading = signal(true);
  error = signal('');
  reload = signal(0);
  increment = (n: number) => n + 1;
  stale = computed(
    () => this.store.clock() - (this.store.row(this.id())()?.ts ?? 0) > CONFIG.staleMs,
  );
  constructor() {
    effect((onCleanup) => {
      const id = this.id();
      this.reload();
      const abort = new AbortController();
      this.chartData.set({ id, points: [] });
      this.loading.set(true);
      this.error.set('');
      // One point per second bounds storage independently of incoming event rate.
      const seconds = new Map<number, Point>();
      const add = (point: Point, live: boolean) => {
        const key = Math.floor(point.ts / 1000);
        if (live || !seconds.has(key)) seconds.set(key, point);
      };
      const subscription = this.store.events.subscribe((e) => {
        if (e.serviceId === id) add(e, true);
      });
      const reset = this.store.resetHistory.subscribe(() => this.reload.update(this.increment));
      const timer = setInterval(() => {
        for (const [key] of seconds)
          if (key * 1000 < Date.now() - CONFIG.windowMs) seconds.delete(key);
        this.chartData.set({ id, points: reducePoints([...seconds.values()], Date.now()) });
      }, CONFIG.chartMs);
      void this.auth.api
        .request<{ serviceId: string; points: Point[] }>(
          `/metrics/history?serviceId=${id}&minutes=15`,
          {},
          abort.signal,
        )
        .then((result) => {
          if (abort.signal.aborted) return;
          if (
            result.serviceId !== id ||
            !result.points.every((p) =>
              ['ts', 'p95', 'errRate', 'rps'].every((k) => Number.isFinite(p[k as keyof Point])),
            )
          )
            throw new Error('Invalid history response');
          result.points.forEach((p) => add(p, false));
          this.chartData.set({ id, points: reducePoints([...seconds.values()], Date.now()) });
          this.loading.set(false);
        })
        .catch((e) => {
          if (!abort.signal.aborted) {
            this.error.set(`History unavailable: ${String(e)}`);
            this.loading.set(false);
          }
        });
      onCleanup(() => {
        abort.abort();
        subscription.unsubscribe();
        reset.unsubscribe();
        clearInterval(timer);
      });
    });
  }
  switchService(event: Event): void {
    void this.router.navigate(['/services', (event.target as HTMLSelectElement).value]);
  }
}
