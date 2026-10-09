import { ChangeDetectionStrategy, Component, signal } from '@angular/core';
import { FormsModule, NgForm } from '@angular/forms';
@Component({
  selector: 'app-settings',
  standalone: true,
  imports: [FormsModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: ` <h1>Settings</h1>
    <form #form="ngForm" (ngSubmit)="save(form)" class="stack narrow">
      <label
        >Operator display name
        <input
          name="displayName"
          [(ngModel)]="displayName"
          required
          minlength="2"
          maxlength="80" /></label
      ><label><input name="compact" type="checkbox" [(ngModel)]="compact" /> Compact spacing</label
      ><button type="submit" [disabled]="form.invalid">Save preferences</button>
      <p role="status">{{ message() }}</p>
    </form>`,
})
export class Settings {
  displayName = '';
  compact = false;
  message = signal('');
  constructor() {
    try {
      const saved = JSON.parse(localStorage.getItem('fleet-preferences') ?? '{}') as {
        displayName?: string;
        compact?: boolean;
      };
      this.displayName = saved.displayName ?? '';
      this.compact = saved.compact ?? false;
    } catch {
      this.message.set('Saved settings could not be read.');
    }
  }
  save(form: NgForm): void {
    if (form.invalid) return;
    try {
      localStorage.setItem(
        'fleet-preferences',
        JSON.stringify({ displayName: this.displayName, compact: this.compact }),
      );
      document.body.classList.toggle('compact', this.compact);
      this.message.set('Preferences saved on this device.');
      form.form.markAsPristine();
    } catch {
      this.message.set('Storage is unavailable; preferences were not saved.');
    }
  }
}
