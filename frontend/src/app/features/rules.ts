import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { CanDeactivateFn } from '@angular/router';
import { MatButtonModule } from '@angular/material/button';
import { Observable } from 'rxjs';
import { Auth } from '../core/auth.service';
import { FleetStore } from '../core/fleet.store';
import { Condition, Rule } from '../core/models';
import { meaningfulName, metricThreshold, uniqueName } from '../core/validators';
@Component({
  selector: 'app-rules',
  host: { '(window:beforeunload)': 'beforeUnload($event)' },
  standalone: true,
  imports: [ReactiveFormsModule, MatButtonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <h1>Alert rules</h1>
    <p>
      All conditions must hold for their own duration on the same service. Gaps over 10 seconds
      reset duration timers.
    </p>
    @if (store.rulesError()) {
      <p role="alert">
        {{ store.rulesError() }} <button (click)="store.loadRules()">Retry</button>
      </p>
    }
    <ul>
      @for (rule of store.rules(); track rule.id) {
        <li>
          {{ rule.name }} <button (click)="edit(rule)" [disabled]="saving()">Edit</button>
          <button (click)="remove(rule)" [disabled]="saving()">Delete</button>
        </li>
      } @empty {
        <li>No saved rules.</li>
      }
    </ul>
    <h2>{{ editing() ? 'Edit rule' : 'Create rule' }}</h2>
    <form [formGroup]="form" (ngSubmit)="save()" class="stack">
      <label
        >Rule name <input formControlName="name" maxlength="80" aria-describedby="name-help"
      /></label>
      <small id="name-help">3–80 nonblank characters; names must be unique.</small>
      @if (form.controls.name.pending) {
        <p role="status">Checking name…</p>
      }
      @if (form.controls.name.touched && form.controls.name.invalid) {
        <p role="alert">Name is invalid, already taken, or availability could not be checked.</p>
      }
      <div formArrayName="conditions">
        @for (condition of conditions.controls; track condition; let i = $index) {
          <fieldset [formGroupName]="i">
            <legend>Condition {{ i + 1 }}</legend>
            <div class="controls">
              <label
                >Metric
                <select formControlName="metric">
                  <option value="p95">p95 latency</option>
                  <option value="errRate">Error rate</option>
                  <option value="rps">Throughput</option>
                </select></label
              >
              <label
                >Operator
                <select formControlName="operator">
                  <option value=">">&gt;</option>
                  <option value=">=">&gt;=</option>
                  <option value="<">&lt;</option>
                  <option value="<=">&lt;=</option>
                </select></label
              >
              <label
                >Threshold ({{
                  condition.controls.metric.value === 'errRate'
                    ? '%'
                    : condition.controls.metric.value === 'p95'
                      ? 'ms'
                      : 'requests/s'
                }})
                <input
                  type="number"
                  formControlName="threshold"
                  min="0"
                  [max]="condition.controls.metric.value === 'errRate' ? 100 : null"
                  step="any"
              /></label>
              <label
                >Duration (seconds)
                <input type="number" formControlName="durationSec" min="0" max="3600"
              /></label>
              <button
                type="button"
                (click)="removeCondition(i)"
                [disabled]="conditions.length === 1"
              >
                Remove condition {{ i + 1 }}
              </button>
            </div>
            @if (condition.invalid && condition.touched) {
              <p role="alert">
                Threshold must be nonnegative (error rate ≤100%). Duration must be 0–3600 seconds.
              </p>
            }
          </fieldset>
        }
      </div>
      <div class="controls">
        <button type="button" (click)="addCondition()" [disabled]="conditions.length >= 20">
          Add condition</button
        ><button
          mat-flat-button
          type="submit"
          [disabled]="form.invalid || form.pending || saving()"
        >
          {{ saving() ? 'Saving…' : 'Save rule' }}</button
        ><button type="button" (click)="newRule()" [disabled]="saving()">New rule / discard</button>
      </div>
      @if (error()) {
        <p role="alert">{{ error() }}</p>
      }
    </form>`,
})
export class Rules {
  store = inject(FleetStore);
  private auth = inject(Auth);
  private fb = inject(FormBuilder).nonNullable;
  editing = signal<string | null>(null);
  saving = signal(false);
  error = signal('');
  form = this.fb.group({
    name: [
      '',
      {
        validators: [meaningfulName],
        asyncValidators: [
          uniqueName(
            (name) =>
              new Observable((subscriber) => {
                const abort = new AbortController();
                void this.auth.api
                  .request<{ available: boolean }>(
                    `/rules/check-name?name=${encodeURIComponent(name)}&excludeId=${this.editing() ?? ''}`,
                    {},
                    abort.signal,
                  )
                  .then(
                    (value) => {
                      subscriber.next(value);
                      subscriber.complete();
                    },
                    (error) => subscriber.error(error),
                  );
                return () => abort.abort();
              }),
          ),
        ],
      },
    ],
    conditions: this.fb.array([this.condition()]),
  });
  get conditions() {
    return this.form.controls.conditions;
  }
  private condition(
    value: Condition = { metric: 'p95', operator: '>', threshold: 500, durationSec: 30 },
  ) {
    return this.fb.group(
      {
        metric: [value.metric, Validators.required],
        operator: [value.operator, Validators.required],
        threshold: [value.threshold, Validators.required],
        durationSec: [
          value.durationSec,
          [Validators.required, Validators.min(0), Validators.max(3600)],
        ],
      },
      { validators: metricThreshold },
    );
  }
  addCondition(): void {
    this.conditions.push(this.condition());
    this.form.markAsDirty();
  }
  removeCondition(index: number): void {
    if (this.conditions.length > 1) {
      this.conditions.removeAt(index);
      this.form.markAsDirty();
    }
  }
  beforeUnload(event: BeforeUnloadEvent): void {
    if (this.auth.user() && (this.form.dirty || this.saving())) {
      event.preventDefault();
      event.returnValue = '';
    }
  }
  canLeave(): boolean {
    return (
      !this.auth.user() ||
      (!this.saving() && (!this.form.dirty || window.confirm('Discard unsaved rule changes?')))
    );
  }
  edit(rule: Rule): void {
    if (!this.canLeave()) return;
    this.editing.set(rule.id);
    this.conditions.clear();
    rule.conditions.forEach((c) => this.conditions.push(this.condition(c)));
    this.form.controls.name.setValue(rule.name);
    this.form.markAsPristine();
  }
  newRule(): void {
    if (!this.canLeave()) return;
    this.editing.set(null);
    this.conditions.clear();
    this.conditions.push(this.condition());
    this.form.controls.name.setValue('');
    this.form.markAsPristine();
  }
  async save(): Promise<void> {
    if (this.form.invalid || this.form.pending || this.saving()) return;
    this.saving.set(true);
    this.error.set('');
    const before = this.store.rules();
    const id = this.editing();
    const key = crypto.randomUUID();
    const body = this.form.getRawValue();
    const optimistic: Rule = {
      ...body,
      id: id ?? `pending-${key}`,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    // The visible list is optimistic; evaluation keeps using the last confirmed rules.
    this.store.rulesChanged(
      id ? before.map((r) => (r.id === id ? optimistic : r)) : [...before, optimistic],
      false,
    );
    try {
      const saved = await this.auth.api.request<Rule>(id ? `/rules/${id}` : '/rules', {
        method: id ? 'PUT' : 'POST',
        headers: { 'Idempotency-Key': key },
        body: JSON.stringify(body),
      });
      this.store.rulesChanged(this.store.rules().map((r) => (r.id === optimistic.id ? saved : r)));
      this.editing.set(saved.id);
      this.form.markAsPristine();
    } catch (e) {
      this.store.rulesChanged(before, false);
      this.error.set(`Save failed; list restored. ${String(e)}`);
    } finally {
      this.saving.set(false);
    }
  }
  async remove(rule: Rule): Promise<void> {
    if (!window.confirm(`Delete ${rule.name}?`)) return;
    this.saving.set(true);
    this.error.set('');
    const before = this.store.rules();
    this.store.rulesChanged(
      before.filter((r) => r.id !== rule.id),
      false,
    );
    try {
      await this.auth.api.request<void>(`/rules/${rule.id}`, { method: 'DELETE' });
      this.store.rulesChanged(this.store.rules());
    } catch (e) {
      this.store.rulesChanged(before, false);
      this.error.set(`Delete failed; list restored. ${String(e)}`);
    } finally {
      this.saving.set(false);
    }
  }
}
export const unsavedRule: CanDeactivateFn<Rules> = (component) => component.canLeave();
