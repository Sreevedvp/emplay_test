import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { DatePipe, JsonPipe } from '@angular/common';
import { RouterLink, RouterOutlet } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { Auth } from './core/auth.service';
import { FleetStore } from './core/fleet.store';
@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, RouterLink, MatButtonModule, DatePipe, JsonPipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <header>
      <a routerLink="/fleet"><strong>Fleet Pulse</strong></a>
      @if (auth.user()) {
        <nav aria-label="Main">
          <a routerLink="/fleet">Fleet</a>
          @if (auth.user()?.role === 'operator') {
            <a routerLink="/rules">Rules</a>
          }
          <a routerLink="/settings">Settings</a>
        </nav>
        <span role="status" aria-live="polite" [class.warning]="store.stale()">{{
          store.status()
        }}</span
        ><small
          >Last event:
          {{ store.lastEvent() ? (store.lastEvent() | date: 'mediumTime') : 'none' }}</small
        ><button mat-button (click)="auth.api.logout()">Log out</button>
      }
    </header>
    <main>
      @if (auth.user()) {
        @if (store.error()) {
          <p role="alert">
            {{ store.error() }} <button (click)="store.retry()">Retry fleet connection</button>
          </p>
        }
        @if (store.notice()) {
          <p role="status" class="notice">
            {{ store.notice() }}
            <button (click)="store.notice.set('')" aria-label="Dismiss notification">
              Dismiss
            </button>
          </p>
        }
        @if (store.rulesError()) {
          <p role="alert">
            {{ store.rulesError() }} <button (click)="store.loadRules()">Retry rules</button>
          </p>
        }
      }
      <router-outlet />
    </main>
    @if (auth.user()) {
      <footer>
        <details>
          <summary>Stream diagnostics</summary>
          <pre>{{ store.counters() | json }}</pre>
          <p>
            Reordered counts arrivals ahead of the cursor. Replay fills confirmed gaps. Snapshot
            resets are explicitly reported.
          </p>
        </details>
      </footer>
    }`,
})
export class App {
  auth = inject(Auth);
  store = inject(FleetStore);
}
